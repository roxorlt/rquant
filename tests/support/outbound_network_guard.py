"""Refuse, and remember, every outbound network attempt a test makes while it is armed.

The notifier cutover rehearsal runs a notifier in `live` mode, which is the one mode in
which the real transport is allowed to speak. A stub at the top of the stack is not enough
evidence that nothing spoke -- a stub proves what was called, not what was not -- so this
guard sits at the bottom: a `sys.addaudithook` hook that sees every `socket.getaddrinfo`,
`socket.gethostbyname*` and `socket.connect` / `socket.sendto` / `socket.sendmsg` the
interpreter performs, from any thread (the legacy PushDeer client posts from a
`ThreadPoolExecutor`), and refuses each one with `OutboundNetworkRefusedError`.

Refusing is not the same as failing the test: the push client catches every exception and
reports "not delivered", so a refusal deep inside it would otherwise vanish. Every refused
attempt is therefore also *recorded*, and the fixture that arms the guard asserts the
record is empty when the test ends. The negative control
(`test_the_guard_catches_a_real_pushdeer_post`) shows the record filling up when the real
client is let through.

`AF_UNIX` addresses (a `str` or `bytes` path) are allowed: the runtime's own receipt
sockets use them and they never leave the host. Audit hooks cannot be removed, so the hook
is installed once per process and does nothing unless a guard is armed.
"""

from __future__ import annotations

import sys
import threading
from dataclasses import dataclass, field
from typing import Any

_NAME_EVENTS = frozenset(
    {
        "socket.getaddrinfo",
        "socket.gethostbyname",
        "socket.gethostbyname_ex",
        "socket.gethostbyaddr",
        "socket.getnameinfo",
    }
)
_ADDRESS_EVENTS = frozenset({"socket.connect", "socket.sendto", "socket.sendmsg"})


class OutboundNetworkRefusedError(ConnectionRefusedError):
    """Raised inside the audited call; the attempt is also recorded on the guard."""


@dataclass
class OutboundNetworkGuard:
    attempts: list[tuple[str, str]] = field(default_factory=list)

    def record(self, event: str, detail: str) -> None:
        self.attempts.append((event, detail))


_LOCK = threading.Lock()
_ARMED: list[OutboundNetworkGuard] = []
_INSTALLED = False


def _address_is_local_socket(address: Any) -> bool:
    return isinstance(address, (str, bytes, bytearray))


def _hook(event: str, arguments: tuple[Any, ...]) -> None:
    if not _ARMED:
        return
    if event in _NAME_EVENTS:
        detail = repr(arguments[:2])
    elif event in _ADDRESS_EVENTS:
        #: all three events are `(socket, address)`
        address = arguments[1] if len(arguments) > 1 else None
        if address is None or _address_is_local_socket(address):
            return
        detail = repr(address)
    else:
        return
    with _LOCK:
        for guard in _ARMED:
            guard.record(event, detail)
    raise OutboundNetworkRefusedError(
        f"outbound network refused during the rehearsal: {event} {detail}"
    )


def _install() -> None:
    global _INSTALLED
    with _LOCK:
        if _INSTALLED:
            return
        sys.addaudithook(_hook)
        _INSTALLED = True


def arm() -> OutboundNetworkGuard:
    _install()
    guard = OutboundNetworkGuard()
    with _LOCK:
        _ARMED.append(guard)
    return guard


def disarm(guard: OutboundNetworkGuard) -> None:
    with _LOCK:
        if guard in _ARMED:
            _ARMED.remove(guard)


__all__ = ["OutboundNetworkGuard", "OutboundNetworkRefusedError", "arm", "disarm"]
