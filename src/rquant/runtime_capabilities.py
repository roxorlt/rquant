"""Systemd credential loading for capability-scoped runtime services."""

from __future__ import annotations

import errno
import os
import re
import stat
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, NamedTuple

from pydantic import StringConstraints, field_serializer, field_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

CAPABILITY_KEYS: Mapping[RuntimeServiceKind, frozenset[str]] = MappingProxyType(
    {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: frozenset(
            {
                "TUSHARE_TOKEN_MAIN",
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
                "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
            }
        ),
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: frozenset(
            {
                "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID",
                "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
            }
        ),
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: frozenset(
            {"TUSHARE_TOKEN_MAIN", "TUSHARE_TOKEN_BACKUP"}
        ),
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
        RuntimeServiceKind.NOTIFIER: frozenset(
            {
                "PUSHDEER_KEYS",
                "PUSHPLUS_TOKENS",
                "PUSHDEER_ENDPOINT",
                "PUSHPLUS_ENDPOINT",
                "PUSHDEER_RECIPIENT_IDS",
                "PUSHPLUS_RECIPIENT_IDS",
            }
        ),
        RuntimeServiceKind.ARTIFACT_RETENTION: frozenset(
            {"RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"}
        ),
    }
)
SECRET_CAPABILITY_KEYS = frozenset(
    {
        "TUSHARE_TOKEN_MAIN",
        "TUSHARE_TOKEN_BACKUP",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL",
    }
)
_MAX_CAPABILITY_BYTES = 1024 * 1024
#: The credential id every runtime unit declares on its `LoadCredentialEncrypted=` line and
#: the `--name=` the root sealer encrypts under. systemd puts the decrypted plaintext at
#: `$CREDENTIALS_DIRECTORY/<this name>`, so the three places have to agree letter for letter.
RUNTIME_CAPABILITY_CREDENTIAL_NAME = "capabilities.json"
#: Where a Linux host says which unit this process belongs to, and where systemd puts the
#: unit's decrypted credentials. `CREDENTIALS_DIRECTORY` is still the only address the
#: credential is ever read from; these two say whether that address is one systemd itself
#: could have produced.
_SYSTEMD_CGROUP_PATH = Path("/proc/self/cgroup")
_SYSTEMD_CREDENTIALS_ROOT = Path("/run/credentials")
#: This process's own mount table, where the credential mount has to show itself.
_SYSTEMD_MOUNT_TABLE = Path("/proc/self/mountinfo")
#: The uid and gid systemd runs as when it decrypts a credential and lays it down: root, on
#: every host, whatever `User=` the unit then executes as. Every check below that means
#: "root" reads this pair instead of a literal 0, and it is the one seam a test that cannot
#: be uid 0 replaces with its own uid. Production never moves it.
_SYSTEMD_DELIVERY_OWNER: tuple[int, int] = (0, 0)
#: systemd mounts the per-unit credential directory itself and never leaves it writable to
#: the unit: 0700 when it is the only reader, 0500/0550 once an ACL admits `User=`.
_CREDENTIAL_DIRECTORY_MODES = frozenset({0o500, 0o550, 0o700})
#: 0400 when the unit runs as root and owns its credentials; 0400 or 0440 root-owned with an
#: ACL for `User=` when it does not. Never a group or world bit beyond that group read.
_CREDENTIAL_FILE_MODES = frozenset({0o400, 0o440})
#: The credential directory is a memory-backed mount that never reaches a disk. systemd has
#: used both over the versions it has shipped `LoadCredentialEncrypted=`, so both are the
#: contract; anything else means the path is not the one systemd made.
_CREDENTIAL_MOUNT_FILESYSTEMS = frozenset({"ramfs", "tmpfs"})
#: The three mount flags systemd sets on that mount, and the reason a non-root reader can be
#: trusted with a root-owned file there: nothing under it can gain privilege, become a
#: device, or be executed.
_CREDENTIAL_MOUNT_OPTIONS = ("nosuid", "nodev", "noexec")
GenerationHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
InstanceName = Annotated[str, StringConstraints(pattern=r"^svc-[0-9a-f]{64}$")]


class RuntimeCapabilityCredential(RuntimeContractModel):
    schema_version: Literal[2] = 2
    service_id: str
    service_kind: RuntimeServiceKind
    instance_name: InstanceName
    bundle_generation: GenerationHash
    capabilities: Mapping[str, str]

    @field_validator("capabilities")
    @classmethod
    def freeze_capabilities(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("capabilities")
    def serialize_capabilities(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class LoadedRuntimeCapabilities(Mapping[str, str]):
    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = MappingProxyType(dict(values))

    def __getitem__(self, key: str) -> str:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"LoadedRuntimeCapabilities(keys={tuple(self._values)!r}, values=<redacted>)"


def _normalize_runtime_capabilities(values: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(values, Mapping):
        raise TypeError("runtime capabilities must be a mapping")
    normalized: dict[str, str] = {}
    for name, value in sorted(values.items()):
        if not isinstance(name, str) or not name:
            raise ValueError("runtime capability names must be nonempty strings")
        if not isinstance(value, str) or not value:
            raise ValueError(f"runtime capability {name} must be a nonempty string")
        if any(character in value for character in ("\x00", "\n", "\r")):
            raise ValueError(f"runtime capability {name} has an unsafe value")
        normalized[name] = value
    return normalized


def serialize_runtime_capabilities(values: Mapping[str, str]) -> bytes:
    return canonical_json_bytes(_normalize_runtime_capabilities(values))


def serialize_runtime_credential(
    *,
    service_id: str,
    service_kind: RuntimeServiceKind,
    instance_name: str,
    bundle_generation: str,
    values: Mapping[str, str],
) -> bytes:
    credential = RuntimeCapabilityCredential(
        service_id=service_id,
        service_kind=service_kind,
        instance_name=instance_name,
        bundle_generation=bundle_generation,
        capabilities=_normalize_runtime_capabilities(values),
    )
    return canonical_json_bytes(credential.model_dump(mode="json"))


def _systemd_unit_name() -> str | None:
    """The systemd unit this process belongs to, out of its own cgroup, or `None`."""

    try:
        text = _SYSTEMD_CGROUP_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        leaf = line.rpartition(":")[2].rpartition("/")[2]
        if leaf.endswith(".service"):
            return leaf
    return None


def _undelivered_credential_reason() -> str | None:
    """Why no credential directory reached this systemd unit's role child, or `None`.

    The two causes need different repairs and used to be indistinguishable, which is what
    made #215 read as "the capability is missing" when the capability had in fact been
    sealed, delivered and decrypted. systemd exports `CREDENTIALS_DIRECTORY` to the unit's
    ExecStart, which is the runtime-exec wrapper; the wrapper then builds the role child's
    environment from an empty dictionary and copies only the names the root-owned profile
    allowlists for that role, so an unlisted name is dropped without a word. The decrypted
    file is still on disk under `/run/credentials/<unit>` either way, and that is the
    evidence that tells the two apart.

    `None` means this process is not running under a systemd unit at all — a bare
    diagnostic run, or the suite. There is no delivery mechanism to accuse there, so the
    caller keeps the behaviour it has always had and lets the role's own builder refuse for
    the capability it actually wanted.
    """

    unit = _systemd_unit_name()
    if unit is None:
        return None
    delivered = _SYSTEMD_CREDENTIALS_ROOT / unit / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    try:
        present = delivered.exists()
    except OSError:  # pragma: no cover - an unreadable /run/credentials is not the diagnosis
        present = False
    if present:
        return (
            f"systemd did load it for unit {unit}, so CREDENTIALS_DIRECTORY was dropped "
            "between the unit and this process: the runtime profile's environment allowlist "
            "for this role does not carry CREDENTIALS_DIRECTORY"
        )
    return (
        f"systemd loaded no {RUNTIME_CAPABILITY_CREDENTIAL_NAME} for unit {unit}: check the "
        "unit's LoadCredentialEncrypted= line and the sealed credstore entry for this instance"
    )


class _MountEntry(NamedTuple):
    """One line of `/proc/self/mountinfo`, reduced to what a credential mount has to prove."""

    point: str
    device: str
    filesystem: str
    options: frozenset[str]


def _decode_mountinfo_path(value: str) -> str:
    """mountinfo octal-escapes the four characters that would otherwise split a field."""

    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _containing_mount(path: Path) -> _MountEntry | None:
    """The mount `path` lives on, out of this process's own mount table, or `None`.

    The longest mount point that covers the path wins, and a later line of equal length wins
    over an earlier one, because that is what an over-mount means. Which entry was chosen is
    then checked against the directory's own `st_dev` by the caller, so a wrong pick cannot
    pass for a right one.
    """

    try:
        table = _SYSTEMD_MOUNT_TABLE.read_text(encoding="utf-8")
    except OSError:
        return None
    candidate = str(path)
    best: _MountEntry | None = None
    for line in table.splitlines():
        left, separator, right = line.partition(" - ")
        fields = left.split()
        tail = right.split()
        if not separator or len(fields) < 6 or not tail:
            continue
        point = _decode_mountinfo_path(fields[4])
        if candidate != point and not candidate.startswith(point.rstrip("/") + "/"):
            continue
        if best is not None and len(point) < len(best.point):
            continue
        best = _MountEntry(
            point=point,
            device=fields[2],
            filesystem=tail[0].lower(),
            options=frozenset(fields[5].split(",")),
        )
    return best


def _credential_mount_fault(directory: Path, device: int) -> str | None:
    """Why the mount under the credential directory is not one systemd made, or `None`.

    This is the check that carries the intent the old `st_uid == os.geteuid()` line was
    standing in for — "only systemd can have put this file here". A unit that runs as
    `User=lighthouse` never owns its own credentials, so ownership cannot say that any more;
    what says it is that the file sits on a root-owned directory on a memory-backed mount
    that is `nosuid,nodev,noexec`, which no unprivileged process can create.
    """

    entry = _containing_mount(directory)
    if entry is None:
        return (
            f"the credential mount cannot be read from {_SYSTEMD_MOUNT_TABLE}, so nothing "
            f"vouches for {directory}"
        )
    expected = f"{os.major(device)}:{os.minor(device)}"
    if entry.device != expected:
        return (
            f"the mount table disagrees with the credential directory: the mount at "
            f"{entry.point} is device {entry.device}, {directory} is on device {expected}"
        )
    if entry.filesystem not in _CREDENTIAL_MOUNT_FILESYSTEMS:
        return (
            f"systemd keeps credentials on a memory-backed mount that never reaches a disk; "
            f"the mount at {entry.point} is {entry.filesystem}"
        )
    missing = tuple(name for name in _CREDENTIAL_MOUNT_OPTIONS if name not in entry.options)
    if missing:
        return (
            f"the credential mount at {entry.point} is missing {', '.join(missing)}: systemd "
            f"mounts it {','.join(_CREDENTIAL_MOUNT_OPTIONS)}, observed "
            f"{','.join(sorted(entry.options))}"
        )
    return None


def _mode_list(modes: frozenset[int]) -> str:
    return " or ".join(f"0o{value:04o}" for value in sorted(modes))


def _credential_directory_fault(directory: Path) -> str | None:
    """Why this is not the directory systemd delivered this unit's credentials into.

    Five things have to hold at once, and each is one clause of what
    `LoadCredentialEncrypted=` promises: the address is `/run/credentials/<unit>`; that unit
    is this one; the directory belongs to root rather than to whoever runs the service; its
    mode lets nobody but root write; and it sits on systemd's own memory-backed mount rather
    than on a lookalike an unprivileged user could have made. Nothing here reads the file.
    """

    delivery_uid, delivery_gid = _SYSTEMD_DELIVERY_OWNER
    unit = directory.name
    if directory.parent != _SYSTEMD_CREDENTIALS_ROOT or not unit.endswith(".service"):
        return (
            f"a systemd credential directory is {_SYSTEMD_CREDENTIALS_ROOT}/<unit>.service, "
            f"observed {directory}"
        )
    running = _systemd_unit_name()
    if running is not None and running != unit:
        return (
            f"the credential directory belongs to unit {unit} while this process runs under "
            f"{running}: a unit may only read the credentials systemd loaded for it"
        )
    try:
        observed = os.lstat(directory)
    except OSError as exc:
        return f"the credential directory {directory} cannot be inspected: {exc.strerror}"
    if not stat.S_ISDIR(observed.st_mode):
        return f"the credential directory {directory} is not a directory"
    if (observed.st_uid, observed.st_gid) != (delivery_uid, delivery_gid):
        return (
            f"systemd creates the credential directory as {delivery_uid}:{delivery_gid}, "
            f"observed {observed.st_uid}:{observed.st_gid} on {directory}"
        )
    mode = stat.S_IMODE(observed.st_mode)
    if mode not in _CREDENTIAL_DIRECTORY_MODES:
        return (
            f"the credential directory mode must be {_mode_list(_CREDENTIAL_DIRECTORY_MODES)}, "
            f"observed 0o{mode:04o} on {directory}"
        )
    return _credential_mount_fault(directory, observed.st_dev)


def _unopenable_credential_reason(path: Path, error: OSError) -> str:
    """What to say when the credential is where it should be but will not open.

    `EACCES` is the shape #215's third break would have produced next: systemd hands a
    `User=`-run service a **root-owned** credential and admits that user through a POSIX ACL
    on the file. If the ACL is not there — wrong `User=`, a hand-copied file, a credential
    laid down for another service — the open is what fails, and saying so with the observed
    owner and mode is the difference between one look and another window lost.
    """

    if error.errno == errno.ENOENT:
        return f"the systemd credential {path} is absent"
    if error.errno != errno.EACCES:
        return "systemd credential is unavailable or unsafe"
    try:
        observed = os.lstat(path)
    except OSError:
        return "systemd credential is unavailable or unsafe"
    return (
        f"the systemd credential {path} is not readable by uid {os.geteuid()}: it is "
        f"{observed.st_uid}:{observed.st_gid} mode 0o{stat.S_IMODE(observed.st_mode):04o} and "
        f"no access control entry admits this uid; LoadCredentialEncrypted admits the unit's "
        f"User= through an ACL, so check User=/Group= against the unit systemd loaded it for"
    )


def _credential_is_absent(path: Path) -> bool:
    """Whether the credential is provably not there — nothing else counts as absent.

    Only `ENOENT` says "systemd loaded some other id into this directory". Every other
    answer, `EACCES` above all, is the delivery ACL failing to admit this uid, and reporting
    that as a missing credential sends the repair to the sealer instead of to `User=`.
    `Path.exists()` cannot be used here at all: it re-raises `EACCES` rather than answering.
    """

    try:
        os.lstat(path)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def _read_private_credential(path: Path) -> bytes:
    """The decrypted plaintext at `path`, if everything about its delivery is systemd's.

    The checks are systemd's own contract for `LoadCredentialEncrypted=`, not a guess at a
    safe-looking file: the directory is `/run/credentials/<this unit>`, root-owned, on
    systemd's memory-backed mount; the file is a regular file with one link, owned by root
    (the ACL delivery, `fd_add_uid_acl_permission(fd, uid, ACL_READ)` over a 0400 file) or
    by this process (the ownership fallback systemd takes where that ACL cannot be held, and
    only over a read-only mount); 0400, or 0440 when root owns it and root's group is the
    only group the mode admits. The old rule — owner must equal the runtime uid, no group
    bit at all — described only the fallback, which is why five units that had their
    credential sealed, delivered and decrypted refused to start.
    """

    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("systemd credential path must be absolute and normalized")
    fault = _credential_directory_fault(candidate.parent)
    if fault is not None:
        raise ValueError(fault)
    delivery_uid, delivery_gid = _SYSTEMD_DELIVERY_OWNER
    runtime_uid = os.geteuid()
    descriptor = -1
    try:
        try:
            descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as exc:
            raise ValueError(_unopenable_credential_reason(candidate, exc)) from exc
        observed = os.fstat(descriptor)
        mode = stat.S_IMODE(observed.st_mode)
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("systemd credential must be a regular file")
        if observed.st_uid not in (delivery_uid, runtime_uid):
            raise ValueError(
                f"systemd credential must be owned by uid {delivery_uid} or by the runtime "
                f"uid {runtime_uid}, observed {observed.st_uid}"
            )
        if observed.st_uid != delivery_uid:
            # systemd hands the file's ownership to the service user only where the backing
            # filesystem cannot hold an ACL, and its own comment says what makes that safe:
            # "only safe if we can then re-mount the whole thing read-only, so that the user
            # can no longer chmod() the file to gain write access" (systemd 252,
            # src/core/execute.c, write_credential). So that is the condition here too —
            # otherwise this is the one branch in which the owner could widen its own mode.
            mount = _containing_mount(candidate.parent)
            if mount is None or "ro" not in mount.options:
                where = mount.point if mount is not None else str(candidate.parent)
                raise ValueError(
                    f"a credential owned by the runtime uid {runtime_uid} is systemd's "
                    f"ownership fallback, which it only takes on a read-only mount; the "
                    f"mount at {where} is not read-only"
                )
        if observed.st_nlink != 1:
            raise ValueError(
                f"systemd credential hardlink count must be one, observed {observed.st_nlink}"
            )
        if mode & 0o007:
            raise ValueError(
                f"systemd credential must not be world accessible, observed 0o{mode:04o}"
            )
        if mode not in _CREDENTIAL_FILE_MODES:
            raise ValueError(
                f"systemd credential mode must be {_mode_list(_CREDENTIAL_FILE_MODES)}, "
                f"observed 0o{mode:04o}"
            )
        if mode & 0o040 and (observed.st_uid, observed.st_gid) != (delivery_uid, delivery_gid):
            raise ValueError(
                f"a group-readable systemd credential is the ACL delivery, which is "
                f"{delivery_uid}:{delivery_gid}; observed {observed.st_uid}:{observed.st_gid}"
            )
        if observed.st_size <= 0 or observed.st_size > _MAX_CAPABILITY_BYTES:
            raise ValueError("systemd credential size is unsafe")
        payload = os.read(descriptor, _MAX_CAPABILITY_BYTES + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) != observed.st_size
            or len(payload) > _MAX_CAPABILITY_BYTES
            or (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("systemd credential changed while being read")
        return payload
    except OSError as exc:
        raise ValueError("systemd credential is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def load_systemd_runtime_capabilities(
    service_kind: RuntimeServiceKind,
    *,
    expected_service_id: str,
    expected_instance: str,
    expected_generation: str | None,
    environ: Mapping[str, str] | None = None,
) -> Mapping[str, str]:
    """The capability values systemd decrypted for this service instance, or refuse.

    `expected_generation` is the **deployment bundle** generation, the only namespace a
    sealed credential is ever bound to: `runtime_deployment_bundle` stamps its own
    `generation_hash` into every plaintext it hands the sealer. It is deliberately not the
    authority chain's generation id, which is what the wrapper forwards as
    `--expected-generation` and which never equals the bundle hash by construction — passing
    that one here would refuse every correctly sealed credential (the same two-namespace
    mistake as #207, and the next wall the credstore roles would have hit after #215).

    `None` means the caller has no deployment bundle at all (Route B publishes none). There
    is then nothing to bind a credential to, so a kind that needs one refuses, and a kind
    that does not may still not quietly accept one.
    """

    if not expected_service_id.strip():
        raise ValueError("expected runtime service id must be nonempty")
    if re.fullmatch(r"svc-[0-9a-f]{64}", expected_instance) is None:
        raise ValueError("expected runtime instance is invalid")
    target = environ if environ is not None else os.environ
    credential_directory = target.get("CREDENTIALS_DIRECTORY", "").strip()
    required = bool(CAPABILITY_KEYS.get(service_kind, frozenset()))
    if not credential_directory and required:
        # The delivery diagnosis comes first and applies on both routes. Which of the two
        # links broke does not depend on whether a deployment bundle exists, and a role
        # started under a systemd unit that was supposed to carry a credential has a broken
        # link either way — putting the Route B branch ahead of this made both messages
        # unreachable there and let the same silent degradation back in.
        reason = _undelivered_credential_reason()
        if reason is not None:
            # Not "the capability is missing": the capability may well have been sealed and
            # decrypted. Say which of the two links is broken so the repair is the right one.
            raise ValueError(
                f"runtime capability credential was not delivered to {service_kind.value}: {reason}"
            )
    if expected_generation is None:
        # Route B publishes no deployment bundle, and the bundle generation is the only
        # namespace a credential is ever sealed in, so on this route no credential for this
        # instance can exist and none could be bound if it did. A kind that needs one
        # therefore cannot run here at all — that is a structural fact rather than a
        # diagnosis, which is why it refuses even outside a systemd unit, unlike the branch
        # above. A kind that needs none may still not quietly accept one.
        if required or credential_directory:
            raise ValueError(
                "runtime capability credential cannot be bound without a deployment generation"
            )
        return LoadedRuntimeCapabilities({})
    if re.fullmatch(r"[0-9a-f]{64}", expected_generation) is None:
        raise ValueError("expected runtime generation must be a lowercase SHA-256")
    if not credential_directory:
        # Route A, nothing delivered, and nothing to accuse: a bare diagnostic run. The
        # role's own builder refuses for the capability it wanted, exactly as it always did.
        return LoadedRuntimeCapabilities({})
    credential_path = Path(credential_directory) / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    try:
        payload = _read_private_credential(credential_path)
    except ValueError as exc:
        # "systemd made this directory but put some other id in it" is only the diagnosis
        # when the directory is one systemd could have made. If the directory itself is
        # wrong, that fault is the answer and this message would bury it.
        if _credential_directory_fault(credential_path.parent) is None and _credential_is_absent(
            credential_path
        ):
            raise ValueError(
                f"the systemd credential directory carries no "
                f"{RUNTIME_CAPABILITY_CREDENTIAL_NAME}: systemd loaded credentials for this "
                f"unit but not this one, so the unit's LoadCredentialEncrypted= name does "
                f"not match what the sealer encrypted under"
            ) from exc
        raise
    credential = strict_model_validate_json(RuntimeCapabilityCredential, payload)
    if credential.service_id != expected_service_id:
        raise ValueError("systemd capability credential service does not match runtime")
    if credential.service_kind is not service_kind:
        raise ValueError("systemd capability credential kind does not match runtime")
    if credential.instance_name != expected_instance:
        raise ValueError("systemd capability credential instance does not match runtime")
    if credential.bundle_generation != expected_generation:
        raise ValueError("systemd capability credential generation does not match runtime")
    decoded = credential.capabilities
    unknown = set(decoded) - CAPABILITY_KEYS.get(service_kind, frozenset())
    if unknown:
        raise ValueError(
            "systemd capability credential contains keys outside the service allowlist"
        )
    loaded: dict[str, str] = {}
    for name, value in sorted(decoded.items()):
        if not isinstance(value, str) or not value:
            raise ValueError("systemd capability values must be nonempty strings")
        existing = target.get(name)
        if existing is not None:
            if existing != value:
                raise ValueError("systemd capability conflicts with the process environment")
            raise ValueError("systemd capability is already present in the process environment")
        loaded[name] = value
    return LoadedRuntimeCapabilities(loaded)


__all__ = [
    "CAPABILITY_KEYS",
    "RUNTIME_CAPABILITY_CREDENTIAL_NAME",
    "LoadedRuntimeCapabilities",
    "RuntimeCapabilityCredential",
    "SECRET_CAPABILITY_KEYS",
    "load_systemd_runtime_capabilities",
    "serialize_runtime_capabilities",
    "serialize_runtime_credential",
]
