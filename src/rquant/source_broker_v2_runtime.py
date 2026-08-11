"""Closed production filesystem and process identities for SourceBroker v2."""

from __future__ import annotations

import os
import pwd
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import ConfigDict, Field, model_validator

from rquant.authority_path_security import AuthorityPathSecurityError, secure_path_metadata
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class SourceBrokerV2RuntimeSecurityError(RuntimeError):
    """The closed SourceBroker v2 production layout cannot be trusted."""


class SourceBrokerV2RootRole(StrEnum):
    CURRENT_CLAIM = "current_claim_monotonic_root"
    SOURCE_QUOTA = "source_quota_monotonic_root"
    REPLAY_LINEAGE = "replay_lineage_monotonic_root"


class SourceBrokerV2ProcessRole(StrEnum):
    CURRENT_CLAIM_AUTHORITY = "current_claim_authority"
    SOURCE_QUOTA_AUTHORITY = "source_quota_authority"
    REPLAY_LINEAGE_AUTHORITY = "replay_lineage_authority"
    CURRENT_CLAIM_ROOT_SERVICE = "current_claim_root_service"
    SOURCE_QUOTA_ROOT_SERVICE = "source_quota_root_service"
    REPLAY_LINEAGE_ROOT_SERVICE = "replay_lineage_root_service"
    SOURCE_DAEMON = "source_daemon"
    SCHEDULER_SOURCE_CLIENT = "scheduler_source_client"


SOURCE_BROKER_V2_LINUX_SYSTEM_USERS = {
    SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY: "rquant-current-claim",
    SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY: "rquant-source-quota",
    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY: "rquant-replay-lineage",
    SourceBrokerV2ProcessRole.CURRENT_CLAIM_ROOT_SERVICE: "rquant-current-root",
    SourceBrokerV2ProcessRole.SOURCE_QUOTA_ROOT_SERVICE: "rquant-quota-root",
    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_ROOT_SERVICE: "rquant-lineage-root",
    SourceBrokerV2ProcessRole.SOURCE_DAEMON: "rquant-source-daemon",
    SourceBrokerV2ProcessRole.SCHEDULER_SOURCE_CLIENT: "rquant-scheduler",
}


class _StrictRuntimeModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceBrokerV2ProcessIdentity(_StrictRuntimeModel):
    role: SourceBrokerV2ProcessRole
    uid: int = Field(strict=True, gt=0)
    gid: int = Field(strict=True, gt=0)


class SourceBrokerV2IdentityMatrix(_StrictRuntimeModel):
    current_claim: SourceBrokerV2ProcessIdentity
    source_quota: SourceBrokerV2ProcessIdentity
    replay_lineage: SourceBrokerV2ProcessIdentity
    current_claim_root: SourceBrokerV2ProcessIdentity
    source_quota_root: SourceBrokerV2ProcessIdentity
    replay_lineage_root: SourceBrokerV2ProcessIdentity
    source_daemon: SourceBrokerV2ProcessIdentity
    scheduler_client: SourceBrokerV2ProcessIdentity

    @model_validator(mode="after")
    def validate_closed_matrix(self) -> Self:
        expected_roles = (
            SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY,
            SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY,
            SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY,
            SourceBrokerV2ProcessRole.CURRENT_CLAIM_ROOT_SERVICE,
            SourceBrokerV2ProcessRole.SOURCE_QUOTA_ROOT_SERVICE,
            SourceBrokerV2ProcessRole.REPLAY_LINEAGE_ROOT_SERVICE,
            SourceBrokerV2ProcessRole.SOURCE_DAEMON,
            SourceBrokerV2ProcessRole.SCHEDULER_SOURCE_CLIENT,
        )
        if tuple(identity.role for identity in self.all) != expected_roles:
            raise ValueError("production process role identity binding changed")
        if len({identity.uid for identity in self.all}) != len(self.all):
            raise ValueError("production role owner UID must be independent and cannot be reused")
        expected_socket_groups = {
            frozenset(
                {
                    SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY,
                    SourceBrokerV2ProcessRole.CURRENT_CLAIM_ROOT_SERVICE,
                }
            ),
            frozenset(
                {
                    SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY,
                    SourceBrokerV2ProcessRole.SOURCE_QUOTA_ROOT_SERVICE,
                }
            ),
            frozenset(
                {
                    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY,
                    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_ROOT_SERVICE,
                }
            ),
            frozenset(
                {
                    SourceBrokerV2ProcessRole.SOURCE_DAEMON,
                    SourceBrokerV2ProcessRole.SCHEDULER_SOURCE_CLIENT,
                }
            ),
        }
        observed_socket_groups: dict[int, set[SourceBrokerV2ProcessRole]] = {}
        for identity in self.all:
            observed_socket_groups.setdefault(identity.gid, set()).add(identity.role)
        if {
            frozenset(roles) for roles in observed_socket_groups.values()
        } != expected_socket_groups:
            raise ValueError(
                "production GID sharing is limited to each exact Unix socket peer pair"
            )
        return self

    @property
    def all(self) -> tuple[SourceBrokerV2ProcessIdentity, ...]:
        return (
            self.current_claim,
            self.source_quota,
            self.replay_lineage,
            self.current_claim_root,
            self.source_quota_root,
            self.replay_lineage_root,
            self.source_daemon,
            self.scheduler_client,
        )

    def root_service(self, role: SourceBrokerV2RootRole) -> SourceBrokerV2ProcessIdentity:
        return {
            SourceBrokerV2RootRole.CURRENT_CLAIM: self.current_claim_root,
            SourceBrokerV2RootRole.SOURCE_QUOTA: self.source_quota_root,
            SourceBrokerV2RootRole.REPLAY_LINEAGE: self.replay_lineage_root,
        }[role]

    def root_consumer(self, role: SourceBrokerV2RootRole) -> SourceBrokerV2ProcessIdentity:
        return {
            SourceBrokerV2RootRole.CURRENT_CLAIM: self.current_claim,
            SourceBrokerV2RootRole.SOURCE_QUOTA: self.source_quota,
            SourceBrokerV2RootRole.REPLAY_LINEAGE: self.replay_lineage,
        }[role]


class SourceBrokerV2AuthorityBinding(_StrictRuntimeModel):
    role: str = Field(min_length=1, max_length=200)
    authority_id: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    schema_version: int = Field(strict=True, ge=1, le=2)
    generation: int = Field(strict=True, ge=1)
    fence_domain_id: str = Field(min_length=1, max_length=200)

    @property
    def binding_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceBrokerV2UnixServicePolicy(_StrictRuntimeModel):
    service_identity: SourceBrokerV2ProcessIdentity
    allowed_peer_identity: SourceBrokerV2ProcessIdentity
    access_gid: int = Field(strict=True, gt=0)
    run_directory: Path
    socket_path: Path
    run_directory_mode: Literal[0o750] = 0o750
    socket_mode: Literal[0o660] = 0o660

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        _require_canonical_absolute(self.run_directory, label="Unix run directory")
        _require_canonical_absolute(self.socket_path, label="Unix socket path")
        if self.socket_path.parent != self.run_directory:
            raise ValueError("Unix socket must be a direct child of its run directory")
        if self.service_identity.uid == self.allowed_peer_identity.uid:
            raise ValueError("Unix service and consumer peer owners must be independent")
        return self

    @property
    def allowed_peer_uid(self) -> int:
        return self.allowed_peer_identity.uid

    @property
    def allowed_peer_gid(self) -> int:
        return self.allowed_peer_identity.gid

    def allows_peer(self, *, uid: int, gid: int) -> bool:
        return uid == self.allowed_peer_uid and gid == self.allowed_peer_gid


class SourceBrokerV2AuthorityRuntimePath(_StrictRuntimeModel):
    binding: SourceBrokerV2AuthorityBinding
    identity: SourceBrokerV2ProcessIdentity
    state_directory: Path
    key_directory: Path
    run_directory: Path
    state_path: Path
    private_key_path: Path
    public_key_path: Path
    root_public_key_path: Path
    socket_path: Path
    manifest_public_key_path: Path | None = None
    separation_state_paths: tuple[Path, ...] = ()
    state_directory_mode: Literal[0o700] = 0o700
    key_directory_mode: Literal[0o700] = 0o700
    run_directory_mode: Literal[0o750] = 0o750
    state_mode: Literal[0o600] = 0o600
    private_key_mode: Literal[0o600] = 0o600
    public_key_mode: Literal[0o600] = 0o600
    socket_mode: Literal[0o660] = 0o660

    @model_validator(mode="after")
    def validate_paths(self) -> Self:
        _validate_private_layout_paths(
            state_directory=self.state_directory,
            key_directory=self.key_directory,
            state_path=self.state_path,
            private_key_path=self.private_key_path,
            public_key_path=self.public_key_path,
        )
        _require_canonical_absolute(self.root_public_key_path, label="root verification key")
        if self.root_public_key_path.parent != self.key_directory:
            raise ValueError("root verification key must belong to the consumer key directory")
        if self.root_public_key_path in {
            self.private_key_path,
            self.public_key_path,
        }:
            raise ValueError("authority and root verification keys must be independent")
        _require_canonical_absolute(self.run_directory, label="authority run directory")
        _require_canonical_absolute(self.socket_path, label="authority socket")
        if self.socket_path.parent != self.run_directory:
            raise ValueError("authority socket must belong to its private run directory")
        if self.manifest_public_key_path is not None:
            _require_canonical_absolute(
                self.manifest_public_key_path,
                label="authority manifest verification key",
            )
            if self.manifest_public_key_path.parent != self.key_directory:
                raise ValueError("authority manifest key must belong to its private key directory")
        for path in self.separation_state_paths:
            _require_canonical_absolute(path, label="authority separation state")
            if path.parent != self.state_directory:
                raise ValueError("authority separation state must belong to its state directory")
        if len(set(self.separation_state_paths)) != len(self.separation_state_paths):
            raise ValueError("authority separation state paths must be independent")
        expected_auxiliary_shape = {
            "current_claim": (True, 0),
            "source_quota": (False, 0),
            "replay_lineage": (False, 2),
        }.get(self.binding.role)
        if expected_auxiliary_shape != (
            self.manifest_public_key_path is not None,
            len(self.separation_state_paths),
        ):
            raise ValueError("authority role-local verification and state shape changed")
        return self

    @property
    def authority_id(self) -> str:
        return self.binding.authority_id

    def unix_policy(
        self,
        scheduler_identity: SourceBrokerV2ProcessIdentity,
    ) -> SourceBrokerV2UnixServicePolicy:
        return SourceBrokerV2UnixServicePolicy(
            service_identity=self.identity,
            allowed_peer_identity=scheduler_identity,
            access_gid=scheduler_identity.gid,
            run_directory=self.run_directory,
            socket_path=self.socket_path,
        )


class SourceBrokerV2ExternalRootRuntime(_StrictRuntimeModel):
    binding: SourceBrokerV2AuthorityBinding
    service_identity: SourceBrokerV2ProcessIdentity
    consumer_identity: SourceBrokerV2ProcessIdentity
    root_store_id: str = Field(min_length=1, max_length=200)
    rollback_domain_id: str = Field(min_length=1, max_length=200)
    state_directory: Path
    key_directory: Path
    run_directory: Path
    state_path: Path
    socket_path: Path
    private_key_path: Path
    public_key_path: Path
    consumer_public_key_path: Path
    state_directory_mode: Literal[0o700] = 0o700
    key_directory_mode: Literal[0o700] = 0o700
    run_directory_mode: Literal[0o750] = 0o750
    socket_mode: Literal[0o660] = 0o660
    state_mode: Literal[0o600] = 0o600
    private_key_mode: Literal[0o600] = 0o600
    public_key_mode: Literal[0o600] = 0o600

    @model_validator(mode="after")
    def validate_paths_and_identity(self) -> Self:
        _validate_private_layout_paths(
            state_directory=self.state_directory,
            key_directory=self.key_directory,
            state_path=self.state_path,
            private_key_path=self.private_key_path,
            public_key_path=self.public_key_path,
        )
        for path, label in (
            (self.run_directory, "root run directory"),
            (self.socket_path, "root socket path"),
            (self.consumer_public_key_path, "consumer root verification key"),
        ):
            _require_canonical_absolute(path, label=label)
        if self.socket_path.parent != self.run_directory:
            raise ValueError("root socket must be a direct child of its run directory")
        if self.binding.role not in {role.value for role in SourceBrokerV2RootRole}:
            raise ValueError("root role is not closed")
        policy = self.unix_policy
        if not policy.allows_peer(
            uid=self.consumer_identity.uid,
            gid=self.consumer_identity.gid,
        ):
            raise ValueError("root peer identity binding changed")
        return self

    @property
    def role(self) -> SourceBrokerV2RootRole:
        return SourceBrokerV2RootRole(self.binding.role)

    @property
    def authority_id(self) -> str:
        return self.binding.authority_id

    @property
    def identity(self) -> SourceBrokerV2ProcessIdentity:
        return self.service_identity

    @property
    def unix_policy(self) -> SourceBrokerV2UnixServicePolicy:
        return SourceBrokerV2UnixServicePolicy(
            service_identity=self.service_identity,
            allowed_peer_identity=self.consumer_identity,
            access_gid=self.consumer_identity.gid,
            run_directory=self.run_directory,
            socket_path=self.socket_path,
        )


class SourceBrokerV2SourceDaemonRuntime(_StrictRuntimeModel):
    binding: SourceBrokerV2AuthorityBinding
    identity: SourceBrokerV2ProcessIdentity
    state_directory: Path
    key_directory: Path
    run_directory: Path
    state_path: Path
    private_key_path: Path
    public_key_path: Path
    next_private_key_path: Path
    next_public_key_path: Path
    socket_path: Path
    state_directory_mode: Literal[0o700] = 0o700
    key_directory_mode: Literal[0o700] = 0o700
    run_directory_mode: Literal[0o750] = 0o750
    state_mode: Literal[0o600] = 0o600
    private_key_mode: Literal[0o600] = 0o600
    public_key_mode: Literal[0o600] = 0o600
    socket_mode: Literal[0o660] = 0o660

    @model_validator(mode="after")
    def validate_paths(self) -> Self:
        _validate_private_layout_paths(
            state_directory=self.state_directory,
            key_directory=self.key_directory,
            state_path=self.state_path,
            private_key_path=self.private_key_path,
            public_key_path=self.public_key_path,
        )
        _require_canonical_absolute(self.run_directory, label="source daemon run directory")
        _require_canonical_absolute(self.socket_path, label="source daemon socket")
        for path, label in (
            (self.next_private_key_path, "source daemon next private key"),
            (self.next_public_key_path, "source daemon next public key"),
        ):
            _require_canonical_absolute(path, label=label)
            if path.parent != self.key_directory:
                raise ValueError("source daemon next keys must belong to its private key directory")
        if (
            len(
                {
                    self.private_key_path,
                    self.public_key_path,
                    self.next_private_key_path,
                    self.next_public_key_path,
                }
            )
            != 4
        ):
            raise ValueError("source daemon current and next key paths must be independent")
        if self.socket_path.parent != self.run_directory:
            raise ValueError("source daemon socket must belong to its run directory")
        return self

    @property
    def authority_id(self) -> str:
        return self.binding.authority_id

    def unix_policy(
        self,
        scheduler_identity: SourceBrokerV2ProcessIdentity,
    ) -> SourceBrokerV2UnixServicePolicy:
        return SourceBrokerV2UnixServicePolicy(
            service_identity=self.identity,
            allowed_peer_identity=scheduler_identity,
            access_gid=scheduler_identity.gid,
            run_directory=self.run_directory,
            socket_path=self.socket_path,
        )


class SourceBrokerV2SchedulerRuntime(_StrictRuntimeModel):
    identity: SourceBrokerV2ProcessIdentity
    state_directory: Path
    key_directory: Path
    saga_state_path: Path
    quota_adapter_state_path: Path
    source_ledger_state_path: Path
    manifest_public_key_path: Path
    current_claim_public_key_path: Path
    replay_lineage_public_key_path: Path
    source_current_public_key_path: Path
    source_next_public_key_path: Path
    state_directory_mode: Literal[0o700] = 0o700
    key_directory_mode: Literal[0o700] = 0o700
    state_mode: Literal[0o600] = 0o600
    public_key_mode: Literal[0o600] = 0o600

    @model_validator(mode="after")
    def validate_paths(self) -> Self:
        for directory, label in (
            (self.state_directory, "scheduler state directory"),
            (self.key_directory, "scheduler verification key directory"),
        ):
            _require_canonical_absolute(directory, label=label)
        state_paths = (
            self.saga_state_path,
            self.quota_adapter_state_path,
            self.source_ledger_state_path,
        )
        key_paths = (
            self.manifest_public_key_path,
            self.current_claim_public_key_path,
            self.replay_lineage_public_key_path,
            self.source_current_public_key_path,
            self.source_next_public_key_path,
        )
        for path in state_paths:
            _require_canonical_absolute(path, label="scheduler state path")
            if path.parent != self.state_directory:
                raise ValueError("scheduler state must belong to its private state directory")
        for path in key_paths:
            _require_canonical_absolute(path, label="scheduler verification key")
            if path.parent != self.key_directory:
                raise ValueError("scheduler key must belong to its private key directory")
        if len(set((*state_paths, *key_paths))) != len(state_paths) + len(key_paths):
            raise ValueError("scheduler state and verification keys must be independent")
        return self


class SourceBrokerV2ProtectedDirectory(_StrictRuntimeModel):
    path: Path
    owner_uid: int = Field(strict=True, ge=0)
    owner_gid: int = Field(strict=True, ge=0)
    mode: Literal[0o700, 0o750]


class SourceBrokerV2ProtectedFile(_StrictRuntimeModel):
    path: Path
    owner_uid: int = Field(strict=True, ge=0)
    owner_gid: int = Field(strict=True, ge=0)
    mode: Literal[0o600]
    purpose: Literal["state", "private-key", "public-key"]


class SourceBrokerV2AuthorityRuntime(_StrictRuntimeModel):
    """Immutable role-separated layout accepted by production composition."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-v2-production-runtime/v2"] = (
        "rquant-source-broker-v2-production-runtime/v2"
    )
    identities: SourceBrokerV2IdentityMatrix
    current_claim: SourceBrokerV2AuthorityRuntimePath
    source_quota: SourceBrokerV2AuthorityRuntimePath
    replay_lineage: SourceBrokerV2AuthorityRuntimePath
    source_daemon: SourceBrokerV2SourceDaemonRuntime
    scheduler_client: SourceBrokerV2SchedulerRuntime
    roots: tuple[SourceBrokerV2ExternalRootRuntime, ...]
    source_authority_current_key_id: str = Field(min_length=1, max_length=200)
    source_authority_next_key_id: str = Field(min_length=1, max_length=200)
    manifest_verification_key_id: str = Field(min_length=1, max_length=200)
    request_timeout_ms: int = Field(default=2_000, strict=True, ge=1, le=30_000)
    busy_timeout_ms: int = Field(default=5_000, strict=True, ge=1, le=30_000)
    source_request_deadline_seconds: float = Field(default=10.0, gt=0, le=30)
    source_max_attempts: int = Field(default=2, strict=True, ge=1, le=5)
    source_takeover_grace_seconds: float = Field(default=5.0, ge=0, le=30)
    executor_lease_seconds: float = Field(default=30.0, gt=0, le=300)

    @model_validator(mode="after")
    def validate_runtime(self) -> Self:
        if self.source_authority_current_key_id == self.source_authority_next_key_id:
            raise ValueError("source authority current and next key ids must differ")
        expected_roles = {role.value for role in SourceBrokerV2RootRole}
        if (
            len(self.roots) != len(expected_roles)
            or {root.binding.role for root in self.roots} != expected_roles
        ):
            raise ValueError("production runtime requires one root for every closed role")
        self.validate_layout()
        minimum_lease = self.source_request_deadline_seconds + self.source_takeover_grace_seconds
        if self.executor_lease_seconds < minimum_lease:
            raise ValueError("executor lease must cover source deadline and takeover grace")
        return self

    @property
    def source_authority_id(self) -> str:
        return self.source_daemon.authority_id

    @property
    def source_socket_path(self) -> Path:
        return self.source_daemon.socket_path

    @property
    def source_authority_private_key_path(self) -> Path:
        return self.source_daemon.private_key_path

    @property
    def source_authority_current_public_key_path(self) -> Path:
        return self.scheduler_client.source_current_public_key_path

    @property
    def source_authority_next_public_key_path(self) -> Path:
        return self.scheduler_client.source_next_public_key_path

    @property
    def manifest_verification_public_key_path(self) -> Path:
        return self.scheduler_client.manifest_public_key_path

    @property
    def saga_state_path(self) -> Path:
        return self.scheduler_client.saga_state_path

    @property
    def source_quota_adapter_state_path(self) -> Path:
        return self.scheduler_client.quota_adapter_state_path

    @property
    def source_authority_ledger_path(self) -> Path:
        return self.scheduler_client.source_ledger_state_path

    @property
    def client_public_key_paths(self) -> tuple[Path, Path]:
        return (
            self.scheduler_client.source_current_public_key_path,
            self.scheduler_client.source_next_public_key_path,
        )

    @property
    def private_layouts(self) -> tuple[object, ...]:
        return (
            self.current_claim,
            self.source_quota,
            self.replay_lineage,
            self.source_daemon,
            *self.roots,
        )

    @property
    def authority_key_pairs(self) -> tuple[tuple[Path, Path], ...]:
        return (
            (self.current_claim.private_key_path, self.current_claim.public_key_path),
            (self.source_quota.private_key_path, self.source_quota.public_key_path),
            (self.replay_lineage.private_key_path, self.replay_lineage.public_key_path),
            (self.source_daemon.private_key_path, self.source_daemon.public_key_path),
            (
                self.source_daemon.next_private_key_path,
                self.source_daemon.next_public_key_path,
            ),
            *((root.private_key_path, root.public_key_path) for root in self.roots),
        )

    def root(self, role: SourceBrokerV2RootRole) -> SourceBrokerV2ExternalRootRuntime:
        for root in self.roots:
            if root.role is role:
                return root
        raise SourceBrokerV2RuntimeSecurityError("closed root role is unavailable")

    @property
    def protected_directories(self) -> tuple[SourceBrokerV2ProtectedDirectory, ...]:
        policies: list[SourceBrokerV2ProtectedDirectory] = []
        for layout in (
            self.current_claim,
            self.source_quota,
            self.replay_lineage,
            self.source_daemon,
            *self.roots,
        ):
            policies.extend(
                (
                    SourceBrokerV2ProtectedDirectory(
                        path=layout.state_directory,
                        owner_uid=layout.identity.uid,
                        owner_gid=layout.identity.gid,
                        mode=0o700,
                    ),
                    SourceBrokerV2ProtectedDirectory(
                        path=layout.key_directory,
                        owner_uid=layout.identity.uid,
                        owner_gid=layout.identity.gid,
                        mode=0o700,
                    ),
                )
            )
        for authority in (self.current_claim, self.source_quota, self.replay_lineage):
            policy = authority.unix_policy(self.identities.scheduler_client)
            policies.append(
                SourceBrokerV2ProtectedDirectory(
                    path=authority.run_directory,
                    owner_uid=authority.identity.uid,
                    owner_gid=policy.access_gid,
                    mode=authority.run_directory_mode,
                )
            )
        for service in (*self.roots, self.source_daemon):
            policy = (
                service.unix_policy
                if isinstance(service, SourceBrokerV2ExternalRootRuntime)
                else service.unix_policy(self.identities.scheduler_client)
            )
            policies.append(
                SourceBrokerV2ProtectedDirectory(
                    path=service.run_directory,
                    owner_uid=service.identity.uid,
                    owner_gid=policy.access_gid,
                    mode=service.run_directory_mode,
                )
            )
        policies.extend(
            (
                SourceBrokerV2ProtectedDirectory(
                    path=self.scheduler_client.state_directory,
                    owner_uid=self.scheduler_client.identity.uid,
                    owner_gid=self.scheduler_client.identity.gid,
                    mode=0o700,
                ),
                SourceBrokerV2ProtectedDirectory(
                    path=self.scheduler_client.key_directory,
                    owner_uid=self.scheduler_client.identity.uid,
                    owner_gid=self.scheduler_client.identity.gid,
                    mode=0o700,
                ),
            )
        )
        return tuple(policies)

    @property
    def protected_files(self) -> tuple[SourceBrokerV2ProtectedFile, ...]:
        policies: list[SourceBrokerV2ProtectedFile] = []
        for layout in (
            self.current_claim,
            self.source_quota,
            self.replay_lineage,
            self.source_daemon,
            *self.roots,
        ):
            policies.extend(
                (
                    _file_policy(layout.state_path, layout.identity, "state"),
                    _file_policy(layout.private_key_path, layout.identity, "private-key"),
                    _file_policy(layout.public_key_path, layout.identity, "public-key"),
                )
            )
        for authority in (self.current_claim, self.source_quota, self.replay_lineage):
            policies.append(
                _file_policy(authority.root_public_key_path, authority.identity, "public-key")
            )
            if authority.manifest_public_key_path is not None:
                policies.append(
                    _file_policy(
                        authority.manifest_public_key_path,
                        authority.identity,
                        "public-key",
                    )
                )
            policies.extend(
                _file_policy(path, authority.identity, "state")
                for path in authority.separation_state_paths
            )
        policies.extend(
            (
                _file_policy(
                    self.source_daemon.next_private_key_path,
                    self.source_daemon.identity,
                    "private-key",
                ),
                _file_policy(
                    self.source_daemon.next_public_key_path,
                    self.source_daemon.identity,
                    "public-key",
                ),
            )
        )
        scheduler = self.scheduler_client
        for path in (
            scheduler.saga_state_path,
            scheduler.quota_adapter_state_path,
            scheduler.source_ledger_state_path,
        ):
            policies.append(_file_policy(path, scheduler.identity, "state"))
        for path in (
            scheduler.manifest_public_key_path,
            scheduler.current_claim_public_key_path,
            scheduler.replay_lineage_public_key_path,
            scheduler.source_current_public_key_path,
            scheduler.source_next_public_key_path,
        ):
            policies.append(_file_policy(path, scheduler.identity, "public-key"))
        return tuple(policies)

    def validate_layout(self) -> None:
        _validate_closed_authority_bindings(self)
        if tuple(root.role for root in self.roots) != tuple(SourceBrokerV2RootRole):
            raise SourceBrokerV2RuntimeSecurityError(
                "production external root role order binding changed"
            )
        if self.current_claim.identity != self.identities.current_claim:
            raise SourceBrokerV2RuntimeSecurityError("current claim identity binding changed")
        if self.source_quota.identity != self.identities.source_quota:
            raise SourceBrokerV2RuntimeSecurityError("source quota identity binding changed")
        if self.replay_lineage.identity != self.identities.replay_lineage:
            raise SourceBrokerV2RuntimeSecurityError("replay lineage identity binding changed")
        if self.source_daemon.identity != self.identities.source_daemon:
            raise SourceBrokerV2RuntimeSecurityError("source daemon identity binding changed")
        if self.scheduler_client.identity != self.identities.scheduler_client:
            raise SourceBrokerV2RuntimeSecurityError("scheduler identity binding changed")
        for role in SourceBrokerV2RootRole:
            root = self.root(role)
            if root.service_identity != self.identities.root_service(
                role
            ) or root.consumer_identity != self.identities.root_consumer(role):
                raise SourceBrokerV2RuntimeSecurityError("root service peer identity changed")
        expected_root_key_copies = {
            SourceBrokerV2RootRole.CURRENT_CLAIM: self.current_claim.root_public_key_path,
            SourceBrokerV2RootRole.SOURCE_QUOTA: self.source_quota.root_public_key_path,
            SourceBrokerV2RootRole.REPLAY_LINEAGE: self.replay_lineage.root_public_key_path,
        }
        for role, expected_path in expected_root_key_copies.items():
            if self.root(role).consumer_public_key_path != expected_path:
                raise SourceBrokerV2RuntimeSecurityError(
                    "external root consumer verification key binding changed"
                )

        directories = tuple(policy.path for policy in self.protected_directories)
        files = tuple(policy.path for policy in self.protected_files)
        sockets = (
            self.current_claim.socket_path,
            self.source_quota.socket_path,
            self.replay_lineage.socket_path,
            self.source_daemon.socket_path,
            *(root.socket_path for root in self.roots),
        )
        if len(set(directories)) != len(directories):
            raise SourceBrokerV2RuntimeSecurityError(
                "production role directories must be independent"
            )
        if len(set(files)) != len(files):
            raise SourceBrokerV2RuntimeSecurityError("production role files must be independent")
        if len(set(sockets)) != len(sockets):
            raise SourceBrokerV2RuntimeSecurityError("production sockets must be independent")
        for index, left in enumerate(directories):
            for right in directories[index + 1 :]:
                if left.is_relative_to(right) or right.is_relative_to(left):
                    raise SourceBrokerV2RuntimeSecurityError(
                        "production role directories must not contain each other"
                    )
        _validate_closed_layout(self)

    def validate_filesystem(
        self,
        *,
        require_key_files: bool = True,
        require_state_files: bool = False,
    ) -> None:
        self.validate_layout()
        self._validate_policies(
            directories=self.protected_directories,
            files=tuple(
                policy
                for policy in self.protected_files
                if (policy.purpose == "state" and require_state_files)
                or (policy.purpose != "state" and require_key_files)
            ),
        )
        self._assert_existing_paths_are_independent()

    def validate_root_filesystem(self, role: SourceBrokerV2RootRole) -> None:
        root = self.root(role)
        self._validate_policies(
            directories=tuple(
                policy
                for policy in self.protected_directories
                if policy.path in {root.state_directory, root.key_directory, root.run_directory}
            ),
            files=(
                _file_policy(root.private_key_path, root.identity, "private-key"),
                _file_policy(root.public_key_path, root.identity, "public-key"),
            ),
        )
        if root.state_path.exists():
            self._validate_policies(
                directories=(),
                files=(_file_policy(root.state_path, root.identity, "state"),),
            )

    def validate_source_daemon_filesystem(self) -> None:
        daemon = self.source_daemon
        self._validate_policies(
            directories=tuple(
                policy
                for policy in self.protected_directories
                if policy.path
                in {daemon.state_directory, daemon.key_directory, daemon.run_directory}
            ),
            files=(
                _file_policy(daemon.private_key_path, daemon.identity, "private-key"),
                _file_policy(daemon.public_key_path, daemon.identity, "public-key"),
                _file_policy(daemon.next_private_key_path, daemon.identity, "private-key"),
                _file_policy(daemon.next_public_key_path, daemon.identity, "public-key"),
            ),
        )

    def validate_authority_filesystem(self, role: SourceBrokerV2ProcessRole) -> None:
        authority = {
            SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY: self.current_claim,
            SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY: self.source_quota,
            SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY: self.replay_lineage,
        }.get(role)
        if authority is None or authority.identity.role is not role:
            raise SourceBrokerV2RuntimeSecurityError("authority filesystem role is invalid")
        selected_paths = {
            authority.private_key_path,
            authority.public_key_path,
            authority.root_public_key_path,
            *authority.separation_state_paths,
        }
        if authority.state_path.exists():
            selected_paths.add(authority.state_path)
        if authority.manifest_public_key_path is not None:
            selected_paths.add(authority.manifest_public_key_path)
        self._validate_policies(
            directories=tuple(
                policy
                for policy in self.protected_directories
                if policy.path
                in {
                    authority.state_directory,
                    authority.key_directory,
                    authority.run_directory,
                }
            ),
            files=tuple(policy for policy in self.protected_files if policy.path in selected_paths),
        )
        self._assert_existing_paths_are_independent()

    def validate_scheduler_filesystem(self) -> None:
        scheduler = self.scheduler_client
        key_paths = {
            scheduler.manifest_public_key_path,
            scheduler.current_claim_public_key_path,
            scheduler.replay_lineage_public_key_path,
            scheduler.source_current_public_key_path,
            scheduler.source_next_public_key_path,
        }
        self._validate_policies(
            directories=tuple(
                policy
                for policy in self.protected_directories
                if policy.path == scheduler.key_directory
            ),
            files=tuple(policy for policy in self.protected_files if policy.path in key_paths),
        )
        self._assert_existing_paths_are_independent()

    def prepare_composition_state(self) -> None:
        selected_directories = {
            self.current_claim.state_directory,
            self.current_claim.key_directory,
            self.source_quota.state_directory,
            self.source_quota.key_directory,
            self.replay_lineage.state_directory,
            self.replay_lineage.key_directory,
            self.scheduler_client.state_directory,
            self.scheduler_client.key_directory,
        }
        selected_files = {
            self.current_claim.state_path,
            self.current_claim.private_key_path,
            self.current_claim.public_key_path,
            self.current_claim.root_public_key_path,
            self.source_quota.state_path,
            self.source_quota.private_key_path,
            self.source_quota.public_key_path,
            self.source_quota.root_public_key_path,
            self.replay_lineage.state_path,
            self.replay_lineage.private_key_path,
            self.replay_lineage.public_key_path,
            self.replay_lineage.root_public_key_path,
            self.scheduler_client.saga_state_path,
            self.scheduler_client.quota_adapter_state_path,
            self.scheduler_client.source_ledger_state_path,
            self.scheduler_client.manifest_public_key_path,
            self.scheduler_client.current_claim_public_key_path,
            self.scheduler_client.replay_lineage_public_key_path,
            self.scheduler_client.source_current_public_key_path,
            self.scheduler_client.source_next_public_key_path,
        }
        self._validate_policies(
            directories=tuple(
                policy
                for policy in self.protected_directories
                if policy.path in selected_directories
            ),
            files=tuple(policy for policy in self.protected_files if policy.path in selected_files),
        )
        self._assert_existing_paths_are_independent()

    @staticmethod
    def _validate_policies(
        *,
        directories: tuple[SourceBrokerV2ProtectedDirectory, ...],
        files: tuple[SourceBrokerV2ProtectedFile, ...],
    ) -> None:
        try:
            for policy in directories:
                secure_path_metadata(
                    policy.path,
                    allowed_ancestor_uids=frozenset({0, policy.owner_uid}),
                    kind="directory",
                    expected_uid=policy.owner_uid,
                    expected_gid=policy.owner_gid,
                    expected_mode=policy.mode,
                )
            for policy in files:
                secure_path_metadata(
                    policy.path,
                    allowed_ancestor_uids=frozenset({0, policy.owner_uid}),
                    kind="file",
                    expected_uid=policy.owner_uid,
                    expected_gid=policy.owner_gid,
                    expected_mode=policy.mode,
                )
        except AuthorityPathSecurityError as exc:
            raise SourceBrokerV2RuntimeSecurityError(
                "production role path owner, mode, or inode is unsafe"
            ) from exc

    def _assert_existing_paths_are_independent(self) -> None:
        existing = tuple(policy.path for policy in self.protected_files if policy.path.exists())
        for index, left in enumerate(existing):
            for right in existing[index + 1 :]:
                try:
                    same = os.path.samefile(left, right)
                except OSError as exc:
                    raise SourceBrokerV2RuntimeSecurityError(
                        "authority filesystem identity is unavailable"
                    ) from exc
                if same:
                    raise SourceBrokerV2RuntimeSecurityError(
                        "production state and key files must use independent inodes"
                    )


def source_broker_v2_default_runtime(
    *,
    root: Path,
    identities: SourceBrokerV2IdentityMatrix,
) -> SourceBrokerV2AuthorityRuntime:
    """Return the deterministic role-separated production layout below ``root``."""

    base = Path(os.path.abspath(root))
    if base != root or not base.is_absolute():
        raise SourceBrokerV2RuntimeSecurityError("runtime root must be canonical and absolute")
    matrix = SourceBrokerV2IdentityMatrix.model_validate(identities, strict=True)

    def directories(stem: str) -> tuple[Path, Path]:
        return base / f"{stem}-state", base / f"{stem}-keys"

    authority_specs = (
        (
            "current-claim",
            "current_claim",
            matrix.current_claim,
            "source-broker-v2-current-claim-authority",
            "source-broker-v2-current-claim-current",
            "source_use_plan_v2",
            2,
        ),
        (
            "source-quota",
            "source_quota",
            matrix.source_quota,
            "source-broker-v2-source-quota-authority",
            "source-broker-v2-source-quota-current",
            "quota_effect",
            1,
        ),
        (
            "replay-lineage",
            "replay_lineage",
            matrix.replay_lineage,
            "source-broker-v2-replay-lineage-authority",
            "source-broker-v2-replay-lineage-current",
            "replay_claim",
            1,
        ),
    )
    authorities: dict[str, SourceBrokerV2AuthorityRuntimePath] = {}
    for stem, role, identity, authority_id, key_id, purpose, schema in authority_specs:
        state_directory, key_directory = directories(stem)
        run_directory = base / f"{stem}-run"
        authorities[role] = SourceBrokerV2AuthorityRuntimePath(
            binding=SourceBrokerV2AuthorityBinding(
                role=role,
                authority_id=authority_id,
                key_id=key_id,
                key_purpose=purpose,
                schema_version=schema,
                generation=1,
                fence_domain_id=f"{stem}-fence-v1",
            ),
            identity=identity,
            state_directory=state_directory,
            key_directory=key_directory,
            run_directory=run_directory,
            state_path=state_directory / "authority.sqlite3",
            private_key_path=key_directory / "authority.private.pem",
            public_key_path=key_directory / "authority.public.pem",
            root_public_key_path=key_directory / "monotonic-root.public.pem",
            socket_path=run_directory / "authority.sock",
            manifest_public_key_path=(
                key_directory / "adapter-manifest.public.pem" if role == "current_claim" else None
            ),
            separation_state_paths=(
                (
                    state_directory / "broker-binding.sqlite3",
                    state_directory / "source-replay-binding.sqlite3",
                )
                if role == "replay_lineage"
                else ()
            ),
        )

    source_state, source_keys = directories("source-daemon")
    source_run = base / "source-daemon-run"
    source_daemon = SourceBrokerV2SourceDaemonRuntime(
        binding=SourceBrokerV2AuthorityBinding(
            role="source_signing",
            authority_id="source-broker-v2-source-authority",
            key_id="source-broker-v2-source-current",
            key_purpose="rquant-source-authority-receipt",
            schema_version=2,
            generation=1,
            fence_domain_id="source-signing-fence-v1",
        ),
        identity=matrix.source_daemon,
        state_directory=source_state,
        key_directory=source_keys,
        run_directory=source_run,
        state_path=source_state / "source-daemon.sqlite3",
        private_key_path=source_keys / "source-authority.private.pem",
        public_key_path=source_keys / "source-authority.public.pem",
        next_private_key_path=source_keys / "source-authority-next.private.pem",
        next_public_key_path=source_keys / "source-authority-next.public.pem",
        socket_path=source_run / "source-authority.sock",
    )

    scheduler_state, scheduler_keys = directories("scheduler")
    scheduler = SourceBrokerV2SchedulerRuntime(
        identity=matrix.scheduler_client,
        state_directory=scheduler_state,
        key_directory=scheduler_keys,
        saga_state_path=scheduler_state / "source-broker-v2-saga.sqlite3",
        quota_adapter_state_path=scheduler_state / "source-quota-adapter.sqlite3",
        source_ledger_state_path=scheduler_state / "source-authority-ledger.sqlite3",
        manifest_public_key_path=scheduler_keys / "manifest-current.public.pem",
        current_claim_public_key_path=scheduler_keys / "current-claim.public.pem",
        replay_lineage_public_key_path=scheduler_keys / "replay-lineage.public.pem",
        source_current_public_key_path=scheduler_keys / "source-current.public.pem",
        source_next_public_key_path=scheduler_keys / "source-next.public.pem",
    )

    root_purposes = {
        SourceBrokerV2RootRole.CURRENT_CLAIM: "current-claim-monotonic-root",
        SourceBrokerV2RootRole.SOURCE_QUOTA: "source-quota-monotonic-root",
        SourceBrokerV2RootRole.REPLAY_LINEAGE: "replay-lineage-monotonic-root",
    }
    consumer_layouts = {
        SourceBrokerV2RootRole.CURRENT_CLAIM: authorities["current_claim"],
        SourceBrokerV2RootRole.SOURCE_QUOTA: authorities["source_quota"],
        SourceBrokerV2RootRole.REPLAY_LINEAGE: authorities["replay_lineage"],
    }
    roots: list[SourceBrokerV2ExternalRootRuntime] = []
    for role in SourceBrokerV2RootRole:
        stem = role.value.replace("_monotonic_root", "-root").replace("_", "-")
        state_directory, key_directory = directories(stem)
        run_directory = base / f"{stem}-run"
        roots.append(
            SourceBrokerV2ExternalRootRuntime(
                binding=SourceBrokerV2AuthorityBinding(
                    role=role.value,
                    authority_id=f"source-broker-v2-{stem}",
                    key_id=f"source-broker-v2-{stem}-current",
                    key_purpose=root_purposes[role],
                    schema_version=1,
                    generation=1,
                    fence_domain_id=f"{stem}-fence-v1",
                ),
                service_identity=matrix.root_service(role),
                consumer_identity=matrix.root_consumer(role),
                root_store_id=f"source-broker-v2-{stem}-store",
                rollback_domain_id=f"source-broker-v2-{stem}-rollback-v1",
                state_directory=state_directory,
                key_directory=key_directory,
                run_directory=run_directory,
                state_path=state_directory / "root.sqlite3",
                socket_path=run_directory / "root.sock",
                private_key_path=key_directory / "root.private.pem",
                public_key_path=key_directory / "root.public.pem",
                consumer_public_key_path=consumer_layouts[role].root_public_key_path,
            )
        )

    return SourceBrokerV2AuthorityRuntime(
        identities=matrix,
        current_claim=authorities["current_claim"],
        source_quota=authorities["source_quota"],
        replay_lineage=authorities["replay_lineage"],
        source_daemon=source_daemon,
        scheduler_client=scheduler,
        roots=tuple(roots),
        source_authority_current_key_id="source-broker-v2-source-current",
        source_authority_next_key_id="source-broker-v2-source-next",
        manifest_verification_key_id="source-broker-v2-manifest-current",
    )


def require_source_broker_v2_linux_system_users(
    identities: SourceBrokerV2IdentityMatrix,
) -> None:
    """Gate deployment on the eight exact Linux system accounts in the runtime."""

    if not os.uname().sysname.lower().startswith("linux"):
        raise SourceBrokerV2RuntimeSecurityError("Linux system-user gate is unavailable")
    matrix = SourceBrokerV2IdentityMatrix.model_validate(identities, strict=True)
    for identity in matrix.all:
        account = SOURCE_BROKER_V2_LINUX_SYSTEM_USERS[identity.role]
        try:
            observed = pwd.getpwnam(account)
        except KeyError as exc:
            raise SourceBrokerV2RuntimeSecurityError(
                f"required SourceBroker v2 system user is missing: {account}"
            ) from exc
        if (observed.pw_uid, observed.pw_gid) != (identity.uid, identity.gid):
            raise SourceBrokerV2RuntimeSecurityError(
                f"SourceBroker v2 system user identity conflicts: {account}"
            )


def _validate_closed_authority_bindings(runtime: SourceBrokerV2AuthorityRuntime) -> None:
    if (
        runtime.source_authority_current_key_id,
        runtime.source_authority_next_key_id,
        runtime.manifest_verification_key_id,
    ) != (
        runtime.source_daemon.binding.key_id,
        "source-broker-v2-source-next",
        "source-broker-v2-manifest-current",
    ):
        raise SourceBrokerV2RuntimeSecurityError("source authority key binding changed")
    expected = (
        (
            runtime.current_claim.binding,
            "current_claim",
            "source-broker-v2-current-claim-authority",
            "source-broker-v2-current-claim-current",
            "source_use_plan_v2",
            2,
            "current-claim-fence-v1",
        ),
        (
            runtime.source_quota.binding,
            "source_quota",
            "source-broker-v2-source-quota-authority",
            "source-broker-v2-source-quota-current",
            "quota_effect",
            1,
            "source-quota-fence-v1",
        ),
        (
            runtime.replay_lineage.binding,
            "replay_lineage",
            "source-broker-v2-replay-lineage-authority",
            "source-broker-v2-replay-lineage-current",
            "replay_claim",
            1,
            "replay-lineage-fence-v1",
        ),
        (
            runtime.source_daemon.binding,
            "source_signing",
            "source-broker-v2-source-authority",
            "source-broker-v2-source-current",
            "rquant-source-authority-receipt",
            2,
            "source-signing-fence-v1",
        ),
    )
    for binding, role, authority_id, key_id, purpose, schema, fence in expected:
        if (
            binding.role,
            binding.authority_id,
            binding.key_id,
            binding.key_purpose,
            binding.schema_version,
            binding.generation,
            binding.fence_domain_id,
        ) != (role, authority_id, key_id, purpose, schema, 1, fence):
            raise SourceBrokerV2RuntimeSecurityError(
                "authority role, identity, key, schema, generation, or fence changed"
            )
    root_purposes = {
        SourceBrokerV2RootRole.CURRENT_CLAIM: "current-claim-monotonic-root",
        SourceBrokerV2RootRole.SOURCE_QUOTA: "source-quota-monotonic-root",
        SourceBrokerV2RootRole.REPLAY_LINEAGE: "replay-lineage-monotonic-root",
    }
    for root in runtime.roots:
        stem = root.role.value.replace("_monotonic_root", "-root").replace("_", "-")
        if (
            root.binding.authority_id != f"source-broker-v2-{stem}"
            or root.binding.key_id != f"source-broker-v2-{stem}-current"
            or root.binding.key_purpose != root_purposes[root.role]
            or root.binding.schema_version != 1
            or root.binding.generation != 1
            or root.binding.fence_domain_id != f"{stem}-fence-v1"
            or root.root_store_id != f"source-broker-v2-{stem}-store"
            or root.rollback_domain_id != f"source-broker-v2-{stem}-rollback-v1"
        ):
            raise SourceBrokerV2RuntimeSecurityError("external root closed binding changed")


def _validate_closed_layout(runtime: SourceBrokerV2AuthorityRuntime) -> None:
    for authority in (runtime.current_claim, runtime.source_quota, runtime.replay_lineage):
        if (
            authority.state_path != authority.state_directory / "authority.sqlite3"
            or authority.private_key_path != authority.key_directory / "authority.private.pem"
            or authority.public_key_path != authority.key_directory / "authority.public.pem"
            or authority.root_public_key_path
            != authority.key_directory / "monotonic-root.public.pem"
            or authority.socket_path != authority.run_directory / "authority.sock"
        ):
            raise SourceBrokerV2RuntimeSecurityError("authority closed path binding changed")
    if (
        runtime.current_claim.manifest_public_key_path
        != runtime.current_claim.key_directory / "adapter-manifest.public.pem"
        or runtime.source_quota.manifest_public_key_path is not None
        or runtime.replay_lineage.manifest_public_key_path is not None
        or runtime.current_claim.separation_state_paths
        or runtime.source_quota.separation_state_paths
        or runtime.replay_lineage.separation_state_paths
        != (
            runtime.replay_lineage.state_directory / "broker-binding.sqlite3",
            runtime.replay_lineage.state_directory / "source-replay-binding.sqlite3",
        )
    ):
        raise SourceBrokerV2RuntimeSecurityError(
            "authority role-local verification or separation path binding changed"
        )
    daemon = runtime.source_daemon
    if (
        daemon.state_path != daemon.state_directory / "source-daemon.sqlite3"
        or daemon.private_key_path != daemon.key_directory / "source-authority.private.pem"
        or daemon.public_key_path != daemon.key_directory / "source-authority.public.pem"
        or daemon.next_private_key_path
        != daemon.key_directory / "source-authority-next.private.pem"
        or daemon.next_public_key_path != daemon.key_directory / "source-authority-next.public.pem"
        or daemon.socket_path != daemon.run_directory / "source-authority.sock"
    ):
        raise SourceBrokerV2RuntimeSecurityError("source daemon closed path binding changed")
    for root in runtime.roots:
        if (
            root.state_path != root.state_directory / "root.sqlite3"
            or root.private_key_path != root.key_directory / "root.private.pem"
            or root.public_key_path != root.key_directory / "root.public.pem"
            or root.socket_path != root.run_directory / "root.sock"
        ):
            raise SourceBrokerV2RuntimeSecurityError("external root closed path binding changed")


def _validate_private_layout_paths(
    *,
    state_directory: Path,
    key_directory: Path,
    state_path: Path,
    private_key_path: Path,
    public_key_path: Path,
) -> None:
    for path, label in (
        (state_directory, "role state directory"),
        (key_directory, "role key directory"),
        (state_path, "role state path"),
        (private_key_path, "role private key"),
        (public_key_path, "role public key"),
    ):
        _require_canonical_absolute(path, label=label)
    if state_directory == key_directory:
        raise ValueError("role state and key directories must be independent")
    if state_path.parent != state_directory:
        raise ValueError("role state must belong to its private state directory")
    if private_key_path.parent != key_directory or public_key_path.parent != key_directory:
        raise ValueError("role keys must belong to its private key directory")
    if len({state_path, private_key_path, public_key_path}) != 3:
        raise ValueError("role state and key paths must be independent")


def _file_policy(
    path: Path,
    identity: SourceBrokerV2ProcessIdentity,
    purpose: Literal["state", "private-key", "public-key"],
) -> SourceBrokerV2ProtectedFile:
    return SourceBrokerV2ProtectedFile(
        path=path,
        owner_uid=identity.uid,
        owner_gid=identity.gid,
        mode=0o600,
        purpose=purpose,
    )


def _require_canonical_absolute(path: Path, *, label: str) -> None:
    candidate = Path(os.path.abspath(path))
    if not candidate.is_absolute() or candidate != path or ".." in candidate.parts:
        raise ValueError(f"{label} must be canonical and absolute")


__all__ = [
    "SOURCE_BROKER_V2_LINUX_SYSTEM_USERS",
    "SourceBrokerV2AuthorityBinding",
    "SourceBrokerV2AuthorityRuntime",
    "SourceBrokerV2AuthorityRuntimePath",
    "SourceBrokerV2ExternalRootRuntime",
    "SourceBrokerV2IdentityMatrix",
    "SourceBrokerV2ProcessIdentity",
    "SourceBrokerV2ProcessRole",
    "SourceBrokerV2ProtectedDirectory",
    "SourceBrokerV2ProtectedFile",
    "SourceBrokerV2RootRole",
    "SourceBrokerV2RuntimeSecurityError",
    "SourceBrokerV2SchedulerRuntime",
    "SourceBrokerV2SourceDaemonRuntime",
    "SourceBrokerV2UnixServicePolicy",
    "require_source_broker_v2_linux_system_users",
    "source_broker_v2_default_runtime",
]
