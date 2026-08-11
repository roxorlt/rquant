"""Frozen signal-routing policy resolution for live runtime services."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

from pydantic import Field, StrictBool, StringConstraints

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import RoutingDecision

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_MAX_POLICY_BYTES = 1024 * 1024
_REASON_PATTERN = r"^[a-z0-9][a-z0-9_.-]{0,127}$"


class RoutingPolicyError(RuntimeError):
    """A frozen routing policy cannot be loaded safely."""


class RoutingPolicyIntegrityError(RoutingPolicyError):
    """The policy path, identity, timestamp, or fingerprint is unsafe."""


class RoutingPolicyConflictError(RoutingPolicyError):
    """The policy contains duplicate or conflicting delivery targets."""


class RoutingPolicyRule(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    action: SignalAction
    recipient_id: str = Field(min_length=1)
    channel: DeliveryChannel
    enabled: StrictBool


class RoutingPolicyDocument(RuntimeContractModel):
    default_no_target_reason: str = Field(pattern=_REASON_PATTERN)
    rules: tuple[RoutingPolicyRule, ...]


RouteKey = tuple[str, str, SignalAction]


@dataclass(frozen=True, slots=True)
class FrozenRoutingPolicyResolver:
    source_path: Path
    content_sha256: str
    routing_policy_fingerprint: str
    default_no_target_reason: str
    _routes: Mapping[RouteKey, tuple[DeliveryTarget, ...]]

    @classmethod
    def from_document(
        cls,
        *,
        source_path: Path,
        content_sha256: str,
        policy: RoutingPolicyDocument,
    ) -> FrozenRoutingPolicyResolver:
        routes: dict[
            RouteKey,
            list[DeliveryTarget],
        ] = defaultdict(list)
        target_keys: set[tuple[str, str, SignalAction, str, DeliveryChannel]] = set()
        for rule in policy.rules:
            target_key = (
                rule.strategy_id,
                rule.strategy_version,
                rule.action,
                rule.recipient_id,
                rule.channel,
            )
            if target_key in target_keys:
                raise RoutingPolicyConflictError(
                    "routing policy contains a duplicate or conflicting target"
                )
            target_keys.add(target_key)
            if rule.enabled:
                routes[(rule.strategy_id, rule.strategy_version, rule.action)].append(
                    DeliveryTarget(
                        recipient_id=rule.recipient_id,
                        channel=rule.channel,
                    )
                )
        frozen_routes = MappingProxyType(
            {
                key: tuple(targets)
                for key, targets in sorted(
                    routes.items(),
                    key=lambda item: (
                        item[0][0],
                        item[0][1],
                        item[0][2].value,
                    ),
                )
            }
        )
        return cls(
            source_path=source_path,
            content_sha256=content_sha256,
            routing_policy_fingerprint=content_sha256,
            default_no_target_reason=policy.default_no_target_reason,
            _routes=frozen_routes,
        )

    def __call__(self, signal: SignalEnvelope) -> RoutingDecision:
        targets = self._routes.get(
            (signal.strategy_id, signal.strategy_version, signal.action),
            (),
        )
        if not targets:
            return RoutingDecision.no_target(
                routing_policy_fingerprint=self.routing_policy_fingerprint,
                reason_code=self.default_no_target_reason,
            )
        return RoutingDecision.route(
            routing_policy_fingerprint=self.routing_policy_fingerprint,
            targets=targets,
        )


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_uid,
    )


def _require_policy_path(path: Path) -> Path:
    candidate = Path(path)
    if (
        not candidate.is_absolute()
        or candidate != Path(os.path.abspath(candidate))
        or candidate.suffix.lower() != ".json"
    ):
        raise RoutingPolicyIntegrityError(
            "routing policy path must be an absolute normal JSON path"
        )
    return candidate


def _open_directory_no_symlinks(path: Path) -> int:
    try:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise RoutingPolicyIntegrityError(
            f"routing policy directory is unavailable: {path.anchor}"
        ) from exc
    traversed = Path(path.anchor)
    try:
        for component in path.parts[1:]:
            traversed /= component
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except OSError as exc:
                raise RoutingPolicyIntegrityError(
                    f"routing policy directory is unavailable: {traversed}"
                ) from exc
            if stat.S_ISLNK(before.st_mode):
                raise RoutingPolicyIntegrityError(
                    f"routing policy path contains a symlink: {traversed}"
                )
            if not stat.S_ISDIR(before.st_mode):
                raise RoutingPolicyIntegrityError(
                    f"routing policy path component is not a directory: {traversed}"
                )
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                raise RoutingPolicyIntegrityError(
                    f"routing policy directory changed while opening: {traversed}"
                ) from exc
            opened = os.fstat(child)
            if (opened.st_dev, opened.st_ino, opened.st_mode) != (
                before.st_dev,
                before.st_ino,
                before.st_mode,
            ):
                os.close(child)
                raise RoutingPolicyIntegrityError(
                    f"routing policy directory identity changed: {traversed}"
                )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_frozen_policy(path: Path, *, observed_at: datetime) -> bytes:
    parent_descriptor = _open_directory_no_symlinks(path.parent)
    descriptor = -1
    try:
        try:
            before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise RoutingPolicyIntegrityError(
                f"routing policy file is unavailable: {path}"
            ) from exc
        if stat.S_ISLNK(before.st_mode):
            raise RoutingPolicyIntegrityError(f"routing policy file is a symlink: {path}")
        if not stat.S_ISREG(before.st_mode):
            raise RoutingPolicyIntegrityError(f"routing policy file is not regular: {path}")
        if before.st_mode & 0o222:
            raise RoutingPolicyIntegrityError("routing policy file must be read-only")
        if before.st_nlink != 1:
            raise RoutingPolicyIntegrityError("routing policy file must be single-link")
        if before.st_mtime_ns > int(observed_at.timestamp() * 1_000_000_000):
            raise RoutingPolicyIntegrityError("routing policy file timestamp is in the future")
        if before.st_size <= 0 or before.st_size > _MAX_POLICY_BYTES:
            raise RoutingPolicyIntegrityError("routing policy file size is unsafe")
        try:
            descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent_descriptor)
        except OSError as exc:
            raise RoutingPolicyIntegrityError(
                f"routing policy file changed while opening: {path}"
            ) from exc
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise RoutingPolicyIntegrityError(
                f"routing policy file identity changed while opening: {path}"
            )
        chunks: list[bytes] = []
        remaining = _MAX_POLICY_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        after = os.fstat(descriptor)
        if _identity(after) != _identity(opened):
            raise RoutingPolicyIntegrityError(f"routing policy file changed while reading: {path}")
        if len(content) != opened.st_size or len(content) > _MAX_POLICY_BYTES:
            raise RoutingPolicyIntegrityError(
                f"routing policy file size changed while reading: {path}"
            )
        return content
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    decoded: dict[str, object] = {}
    for key, value in pairs:
        if key in decoded:
            raise RoutingPolicyConflictError(f"routing policy contains duplicate JSON key: {key}")
        decoded[key] = value
    return decoded


def _decode_policy(content: bytes) -> RoutingPolicyDocument:
    try:
        decoded = json.loads(content, object_pairs_hook=_reject_duplicate_json_keys)
    except UnicodeDecodeError as exc:
        raise RoutingPolicyIntegrityError("routing policy must be UTF-8 JSON") from exc
    except json.JSONDecodeError as exc:
        raise RoutingPolicyIntegrityError("routing policy is not valid JSON") from exc
    return RoutingPolicyDocument.model_validate(decoded)


def load_frozen_routing_policy(
    path: Path,
    *,
    routing_policy_fingerprint: str | None = None,
    observed_at: datetime | None = None,
) -> FrozenRoutingPolicyResolver:
    candidate = _require_policy_path(path)
    effective_observed_at = normalize_aware_utc(observed_at or datetime.now(UTC))
    content = _read_frozen_policy(candidate, observed_at=effective_observed_at)
    content_sha256 = hashlib.sha256(content).hexdigest()
    if routing_policy_fingerprint is not None:
        if _SHA256_PATTERN.fullmatch(routing_policy_fingerprint) is None:
            raise RoutingPolicyIntegrityError(
                "routing policy fingerprint must be a lowercase SHA-256 digest"
            )
        if routing_policy_fingerprint != content_sha256:
            raise RoutingPolicyIntegrityError("routing policy fingerprint does not match content")
    policy = _decode_policy(content)
    return FrozenRoutingPolicyResolver.from_document(
        source_path=candidate,
        content_sha256=content_sha256,
        policy=policy,
    )


__all__ = [
    "FrozenRoutingPolicyResolver",
    "RoutingPolicyConflictError",
    "RoutingPolicyDocument",
    "RoutingPolicyError",
    "RoutingPolicyIntegrityError",
    "RoutingPolicyRule",
    "load_frozen_routing_policy",
]
