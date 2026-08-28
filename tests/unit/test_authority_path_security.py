"""Direct cover for the shared authority path walk.

The walk is consumed by the R07 evidence cache and three runtime surfaces, and until now
every property it holds was only asserted through one of those consumers. Two of them are
not observable that way at all: `O_NOFOLLOW` is backstopped by a later `require_unchanged`
comparison, so removing it leaves the consumer suites green, and the sticky-ancestor
relaxation is a widening switch whose refusal at the filesystem root nothing exercised.
"""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

import pytest

from rquant import authority_path_security as authority


def _private_tree(root: Path) -> Path:
    entry = root / "nested" / "entry.json"
    entry.parent.mkdir(mode=0o700, parents=True)
    entry.write_bytes(b"{}")
    entry.chmod(0o600)
    root.chmod(0o700)
    return entry


def test_every_descriptor_in_the_walk_is_opened_with_o_nofollow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing the flag is caught here rather than by the consumer that happens to notice.

    A later `require_unchanged` comparison rejects a swapped path anyway, so the consumer
    suites stay green with `O_NOFOLLOW` deleted. That makes the flag a layer a refactor can
    drop silently; this pins it at the layer that owns it.
    """

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    entry = _private_tree(root)
    opened_flags: list[int] = []
    real_open = os.open

    def recording_open(path: object, flags: int, *arguments: object, **keywords: object) -> int:
        opened_flags.append(flags)
        return real_open(path, flags, *arguments, **keywords)  # type: ignore[arg-type]

    monkeypatch.setattr(authority.os, "open", recording_open)

    payload = authority.read_secure_regular_file(
        entry,
        trusted_root=root,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
        allowed_modes=frozenset({0o600}),
        max_bytes=64,
    )

    assert payload == b"{}"
    # The trusted root, the one directory below it, and the entry itself.
    assert len(opened_flags) == 3
    assert all(flags & os.O_NOFOLLOW for flags in opened_flags)


def test_sticky_ancestors_are_never_tolerated_for_the_filesystem_root(tmp_path: Path) -> None:
    """The relaxation exists for a lab root under /tmp and must not reach a production walk."""

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    entry = _private_tree(root)
    arguments = {
        "expected_uid": os.geteuid(),
        "expected_gid": os.getegid(),
        "allowed_modes": frozenset({0o600}),
        "max_bytes": 64,
    }

    with pytest.raises(ValueError, match="explicit lab or test root"):
        authority.open_secure_regular_file_lease(
            entry,
            trusted_root=Path("/"),
            allow_sticky_world_writable_ancestors=True,
            **arguments,  # type: ignore[arg-type]
        )

    # It is the switch that is refused, not the root: with it off the same call gets past
    # this guard and into the walk, where whatever the real ancestor chain looks like on
    # this host decides. Either answer is fine here; a ValueError from the guard is not.
    with suppress(authority.AuthorityPathSecurityError):
        authority.open_secure_regular_file_lease(
            entry,
            trusted_root=Path("/"),
            allow_sticky_world_writable_ancestors=False,
            **arguments,  # type: ignore[arg-type]
        ).close()

    # And under an explicit root the relaxation is accepted.
    with authority.open_secure_regular_file_lease(
        entry,
        trusted_root=root,
        allow_sticky_world_writable_ancestors=True,
        **arguments,  # type: ignore[arg-type]
    ) as lease:
        assert lease.read_all(max_bytes=64) == b"{}"
