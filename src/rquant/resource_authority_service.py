"""Startable resource-journal authority service over the external monotonic root."""

from __future__ import annotations

import grp
import os
import pwd
import re
import socket
import stat
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol, Self, TypeVar

from pydantic import Field, model_validator

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    read_secure_regular_file,
    secure_create_regular_file,
    secure_path_metadata,
)
from rquant.external_monotonic_root import (
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import (
    EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
    ClosedExternalMonotonicRootVerifier,
    ExternalMonotonicRootUnixService,
    ExternalRootServiceConfiguration,
    ExternalRootStoredState,
    OpenSslExternalMonotonicRootSigner,
    PersistentExternalMonotonicRootBackend,
)
from rquant.lab_resource_authority_adapter import (
    ExternalResourceJournalMonotonicRootAdapter,
    ResourceAuthorityAdapterConfig,
    ResourceAuthorityAdapterIdentity,
    ResourceAuthorityJournalClient,
    ResourceAuthorityJournalSocketServer,
    ResourceJournalExternalRootReceipt,
    compose_production_resource_authority_socket_server,
)
from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
    RESOURCE_JOURNAL_HEAD_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterCheckpoint,
    SQLiteResourceJournalHighWaterAuthority,
    TrustedRoleInventory,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_resource_admission import (
    RESOURCE_OPERATION_KEY_PURPOSE,
    RESOURCE_OPERATION_RECEIPT_NAMESPACE,
    ClosedResourceOperationKeyring,
    SQLiteResourceAdmissionAuthority,
)
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json

_RESOURCE_OPERATION_SIGNING_NAMESPACES = frozenset(
    {
        RESOURCE_OPERATION_RECEIPT_NAMESPACE,
        "rquant-resource-admission-genesis/v1",
        RESOURCE_JOURNAL_HEAD_NAMESPACE,
    }
)
_RESOURCE_ROOT_SIGNING_NAMESPACES = frozenset(
    {
        RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
        EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
    }
)
_MAX_DAEMON_CONFIGURATION_BYTES = 2 * 1024 * 1024
ConfigurationT = TypeVar("ConfigurationT", bound=RuntimeContractModel)
EXTERNAL_ROOT_SERVICE_USER = "rquant-external-root"
EXTERNAL_ROOT_CLIENT_GROUP = "rquant-root-client"
RESOURCE_AUTHORITY_SERVICE_USER = "rquant-resource-authority"
RESOURCE_AUTHORITY_CLIENT_GROUP = "rquant-resource-client"
RESOURCE_AUTHORITY_APPLICATION_USER = "lighthouse"
EXTERNAL_ROOT_ENVIRONMENT_PATH = Path("/etc/rquant/external-root.env")
RESOURCE_AUTHORITY_ENVIRONMENT_PATH = Path("/etc/rquant/resource-authority.env")
EXTERNAL_ROOT_ENVIRONMENT_KEYS = frozenset(
    {
        "APP_ENV",
        "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH",
    }
)
RESOURCE_AUTHORITY_ENVIRONMENT_KEYS = frozenset(
    {
        "APP_ENV",
        "RQUANT_CODE_COMMIT",
        "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT",
        "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON",
        "RQUANT_LAB_RESOURCE_POLICY_VERSION",
        "RQUANT_LAB_TRADE_CALENDAR_PATH",
        "RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH",
        "RQUANT_RESOURCE_AUTHORITY_STATE_DIR",
    }
)
_ENVIRONMENT_KEY = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_MAX_AUTHORITY_ENVIRONMENT_BYTES = 2 * 1024 * 1024


def _is_canonical_absolute_path(path: Path) -> bool:
    return bool(
        path.is_absolute()
        and ".." not in path.parts
        and path == Path(os.path.abspath(path))
        and path.resolve(strict=False) == path
    )


class ResourceAuthorityServiceError(RuntimeError):
    """The resource authority daemon cannot establish its closed production binding."""


def load_closed_authority_environment(
    path: Path,
    *,
    allowed_keys: frozenset[str],
    required_keys: frozenset[str],
    trusted_root: Path = Path("/"),
    expected_uid: int = 0,
    expected_gid: int | None = None,
) -> dict[str, str]:
    """Read a non-shell authority EnvironmentFile with an exact key registry."""

    if not required_keys <= allowed_keys or not allowed_keys:
        raise ResourceAuthorityServiceError("authority environment registry is invalid")
    gid = os.getegid() if expected_gid is None else expected_gid
    try:
        payload = read_secure_regular_file(
            path,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid, os.geteuid()}),
            expected_uid=expected_uid,
            expected_gid=gid,
            allowed_modes=frozenset({0o400, 0o440, 0o444, 0o600}),
            max_bytes=_MAX_AUTHORITY_ENVIRONMENT_BYTES,
        )
        text = payload.decode("ascii")
    except (AuthorityPathSecurityError, UnicodeError) as exc:
        raise ResourceAuthorityServiceError(
            "authority environment path or ancestor is unsafe"
        ) from exc
    if not text.endswith("\n") or "\r" in text or "\x00" in text:
        raise ResourceAuthorityServiceError("authority environment is malformed")
    values: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or line.count("=") < 1:
            raise ResourceAuthorityServiceError("authority environment is malformed")
        key, value = line.split("=", 1)
        if (
            _ENVIRONMENT_KEY.fullmatch(key) is None
            or key not in allowed_keys
            or key in values
            or not value
            or any(character.isspace() for character in value)
            or any(token in value for token in ("$", "`", "\\", ";", "&", "|", "<", ">"))
        ):
            raise ResourceAuthorityServiceError("authority environment is not closed")
        values[key] = value
    if frozenset(values) != required_keys:
        raise ResourceAuthorityServiceError("authority environment required keys are missing")
    return values


class ResourceOperationSigner(Protocol):
    issuer: str
    key_id: str
    key_purpose: str
    signature_algorithm: str
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class ResourceOperationVerifier(ResourceOperationSigner, Protocol):
    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool: ...


class TrustedRoleInventoryConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-trusted-role-inventory-config/v1"] = (
        "rquant-trusted-role-inventory-config/v1"
    )
    roles: dict[str, tuple[str, ...]]

    @model_validator(mode="after")
    def validate_closed_inventory(self) -> Self:
        self.to_inventory()
        return self

    def to_inventory(self) -> TrustedRoleInventory:
        return TrustedRoleInventory(
            role_fingerprints={
                purpose: frozenset(fingerprints) for purpose, fingerprints in self.roles.items()
            }
        )


class ExternalMonotonicRootDaemonConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-daemon-config/v1"] = (
        "rquant-external-monotonic-root-daemon-config/v1"
    )
    handler_registry_version: Literal["resource-journal-v1"] = "resource-journal-v1"
    service_configuration: ExternalRootServiceConfiguration
    backend_path: Path
    high_water_authority_id: str = Field(min_length=1, max_length=200)
    private_key_path: Path
    public_key_path: Path
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["resource-journal-high-water"] = RESOURCE_JOURNAL_HIGH_WATER_PURPOSE

    @model_validator(mode="after")
    def validate_closed_root_daemon(self) -> Self:
        paths = (self.backend_path, self.private_key_path, self.public_key_path)
        if any(not _is_canonical_absolute_path(path) for path in paths):
            raise ValueError("external root daemon paths must be absolute and normalized")
        if len(set(paths)) != len(paths):
            raise ValueError("external root daemon stores and keys must be independent")
        if self.service_configuration.role != "resource_journal_monotonic_root":
            raise ValueError("external root daemon role is not registered")
        if not _is_canonical_absolute_path(self.service_configuration.socket_path):
            raise ValueError("external root daemon socket path is not canonical")
        return self


class ResourceAuthorityServiceConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-resource-authority-service-config/v1"] = (
        "rquant-resource-authority-service-config/v1"
    )
    adapter_configuration: ResourceAuthorityAdapterConfig
    external_root_manifest: UnixSocketExternalMonotonicRootManifest
    resource_journal_path: Path
    high_water_cache_path: Path
    trusted_role_inventory: TrustedRoleInventoryConfiguration
    trusted_journal_issuer: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_production_binding(self) -> Self:
        adapter = self.adapter_configuration
        root = adapter.external_root_config
        manifest = self.external_root_manifest
        if adapter.mode != "production" or root is None:
            raise ValueError("resource authority service requires production V2 configuration")
        if (
            root.transport_manifest_hash != manifest.manifest_hash
            or root.role != manifest.role
            or root.root_authority_id != manifest.authority_id
            or root.root_store_id != manifest.store_id
            or root.witness_rollback_domain_id != manifest.rollback_domain_id
        ):
            raise ValueError("resource authority external root manifest conflicts")
        if (
            self.trusted_role_inventory.to_inventory().policy_hash
            != adapter.trusted_role_inventory_hash
        ):
            raise ValueError("resource authority trusted role inventory conflicts")
        if not all(
            _is_canonical_absolute_path(path)
            for path in (self.resource_journal_path, self.high_water_cache_path)
        ):
            raise ValueError("resource authority durable paths must be absolute")
        if self.resource_journal_path == self.high_water_cache_path:
            raise ValueError("resource authority durable stores must be independent")
        return self


class ResourceAuthorityDaemonConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-resource-authority-daemon-config/v1"] = (
        "rquant-resource-authority-daemon-config/v1"
    )
    service_configuration: ResourceAuthorityServiceConfiguration
    operation_private_key_path: Path
    operation_public_key_path: Path
    operation_issuer: str = Field(min_length=1, max_length=200)
    operation_key_id: str = Field(min_length=1, max_length=200)
    root_public_key_path: Path

    @model_validator(mode="after")
    def validate_closed_resource_daemon(self) -> Self:
        paths = (
            self.operation_private_key_path,
            self.operation_public_key_path,
            self.root_public_key_path,
        )
        if any(not _is_canonical_absolute_path(path) for path in paths):
            raise ValueError("resource authority daemon key paths must be absolute and normalized")
        if len(set(paths)) != len(paths):
            raise ValueError("resource authority daemon keys must be distinct")
        if self.operation_issuer != self.service_configuration.trusted_journal_issuer:
            raise ValueError("resource authority operation issuer conflicts")
        return self


class ResourceAuthorityServiceProbe(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-resource-authority-service-probe/v1"] = (
        "rquant-resource-authority-service-probe/v1"
    )
    identity: ResourceAuthorityAdapterIdentity
    capabilities: tuple[Literal["policy", "snapshot", "journal"], ...]

    @model_validator(mode="after")
    def validate_capabilities(self) -> Self:
        if self.capabilities != ("policy", "snapshot", "journal"):
            raise ValueError("resource authority capabilities are not closed")
        return self


def _mode_access_check(
    path: Path,
    requested: int,
    uid: int,
    primary_gid: int,
    supplementary_gids: frozenset[int],
) -> bool:
    metadata = path.lstat()
    if uid == metadata.st_uid:
        available = (stat.S_IMODE(metadata.st_mode) >> 6) & 0o7
    elif metadata.st_gid in {primary_gid, *supplementary_gids}:
        available = (stat.S_IMODE(metadata.st_mode) >> 3) & 0o7
    else:
        available = stat.S_IMODE(metadata.st_mode) & 0o7
    required = (
        (0o4 if requested & os.R_OK else 0)
        | (0o2 if requested & os.W_OK else 0)
        | (0o1 if requested & os.X_OK else 0)
    )
    return available & required == required


def verify_authority_os_isolation(
    root_daemon: ExternalMonotonicRootDaemonConfiguration,
    resource_daemon: ResourceAuthorityDaemonConfiguration,
    *,
    user_lookup: Callable[[str], object] | None = None,
    group_lookup: Callable[[str], object] | None = None,
    metadata_lookup: Callable[[Path], object] | None = None,
    access_check: Callable[[Path, int, int, int, frozenset[int]], bool] | None = None,
) -> tuple[str, ...]:
    """Verify the deployed POSIX authority boundary without mutating it."""

    users = user_lookup or pwd.getpwnam
    groups = group_lookup or grp.getgrnam
    metadata_for = metadata_lookup or (lambda path: path.lstat())
    can_access = access_check or _mode_access_check
    try:
        root_user = users(EXTERNAL_ROOT_SERVICE_USER)
        resource_user = users(RESOURCE_AUTHORITY_SERVICE_USER)
        application_user = users(RESOURCE_AUTHORITY_APPLICATION_USER)
        root_client_group = groups(EXTERNAL_ROOT_CLIENT_GROUP)
        resource_client_group = groups(RESOURCE_AUTHORITY_CLIENT_GROUP)
        root_uid = int(root_user.pw_uid)
        root_gid = int(root_user.pw_gid)
        resource_uid = int(resource_user.pw_uid)
        resource_gid = int(resource_user.pw_gid)
        application_uid = int(application_user.pw_uid)
        application_gid = int(application_user.pw_gid)
        root_client_gid = int(root_client_group.gr_gid)
        resource_client_gid = int(resource_client_group.gr_gid)
        root_members = frozenset(str(value) for value in root_client_group.gr_mem)
        resource_members = frozenset(str(value) for value in resource_client_group.gr_mem)
    except (KeyError, OSError, TypeError, ValueError, AttributeError) as exc:
        raise ResourceAuthorityServiceError(
            "authority system user or group is unavailable"
        ) from exc

    if (
        0 in {root_uid, resource_uid, application_uid}
        or len({root_uid, resource_uid, application_uid}) != 3
        or root_gid != root_client_gid
        or resource_gid != resource_client_gid
        or root_client_gid == resource_client_gid
    ):
        raise ResourceAuthorityServiceError("authority system principal identities conflict")
    if (
        RESOURCE_AUTHORITY_SERVICE_USER not in root_members
        or RESOURCE_AUTHORITY_APPLICATION_USER in root_members
    ):
        raise ResourceAuthorityServiceError("external root client group membership is unsafe")
    if (
        RESOURCE_AUTHORITY_APPLICATION_USER not in resource_members
        or EXTERNAL_ROOT_SERVICE_USER in resource_members
    ):
        raise ResourceAuthorityServiceError("resource authority client group membership is unsafe")

    root_service = root_daemon.service_configuration
    adapter = resource_daemon.service_configuration.adapter_configuration
    if (
        root_service.socket_uid,
        root_service.socket_gid,
        root_service.service_uid,
        root_service.service_gid,
        root_service.allowed_peer_uid,
        root_service.allowed_peer_gid,
        root_service.socket_mode,
        root_service.socket_directory_mode,
    ) != (
        root_uid,
        root_client_gid,
        root_uid,
        root_client_gid,
        resource_uid,
        resource_client_gid,
        0o660,
        0o750,
    ) or (
        adapter.expected_uid,
        adapter.expected_gid,
        adapter.expected_server_uid,
        adapter.expected_server_gid,
        adapter.allowed_peer_uid,
        adapter.allowed_peer_gid,
        adapter.socket_mode,
        adapter.socket_directory_mode,
    ) != (
        resource_uid,
        resource_client_gid,
        resource_uid,
        resource_client_gid,
        application_uid,
        application_gid,
        0o660,
        0o750,
    ):
        raise ResourceAuthorityServiceError("authority socket principal binding conflicts")

    root_runtime = root_service.socket_path.parent
    resource_runtime = adapter.endpoint.parent
    root_state = root_daemon.backend_path.parent
    resource_state = resource_daemon.service_configuration.resource_journal_path.parent
    root_key_root = root_daemon.private_key_path.parent
    resource_key_root = resource_daemon.operation_private_key_path.parent

    def require_metadata(
        path: Path,
        *,
        kind: Literal["directory", "file", "socket"],
        uid: int,
        gid: int,
        mode: int,
        ancestor_uids: frozenset[int],
    ) -> None:
        try:
            if metadata_lookup is None:
                secure_path_metadata(
                    path,
                    trusted_root=Path("/"),
                    allowed_ancestor_uids=ancestor_uids,
                    kind=kind,
                    expected_uid=uid,
                    expected_gid=gid,
                    expected_mode=mode,
                )
                return
            metadata = metadata_for(path)
            actual_mode = int(metadata.st_mode)
            kind_matches = {
                "directory": stat.S_ISDIR,
                "file": stat.S_ISREG,
                "socket": stat.S_ISSOCK,
            }[kind](actual_mode)
            if (
                not kind_matches
                or int(metadata.st_uid) != uid
                or int(metadata.st_gid) != gid
                or stat.S_IMODE(actual_mode) != mode
            ):
                raise ResourceAuthorityServiceError(f"authority {kind} owner or mode is unsafe")
        except ResourceAuthorityServiceError:
            raise
        except (
            AuthorityPathSecurityError,
            OSError,
            TypeError,
            ValueError,
            AttributeError,
        ) as exc:
            raise ResourceAuthorityServiceError(
                f"authority {kind} metadata is unavailable"
            ) from exc

    for path, uid, gid, mode, ancestor_uids in (
        (root_runtime, root_uid, root_client_gid, 0o750, frozenset({0, root_uid})),
        (
            resource_runtime,
            resource_uid,
            resource_client_gid,
            0o750,
            frozenset({0, resource_uid}),
        ),
        (root_state, root_uid, root_client_gid, 0o700, frozenset({0, root_uid})),
        (
            resource_state,
            resource_uid,
            resource_client_gid,
            0o700,
            frozenset({0, resource_uid}),
        ),
        (root_key_root, root_uid, root_client_gid, 0o750, frozenset({0, root_uid})),
        (
            resource_key_root,
            resource_uid,
            resource_client_gid,
            0o750,
            frozenset({0, resource_uid}),
        ),
    ):
        require_metadata(
            path,
            kind="directory",
            uid=uid,
            gid=gid,
            mode=mode,
            ancestor_uids=ancestor_uids,
        )
    for path, uid, gid, mode, ancestor_uids in (
        (
            root_daemon.backend_path,
            root_uid,
            root_client_gid,
            0o600,
            frozenset({0, root_uid}),
        ),
        (
            root_daemon.private_key_path,
            root_uid,
            root_client_gid,
            0o400,
            frozenset({0, root_uid}),
        ),
        (
            root_daemon.public_key_path,
            0,
            root_client_gid,
            0o440,
            frozenset({0, root_uid}),
        ),
        (
            resource_daemon.service_configuration.resource_journal_path,
            resource_uid,
            resource_client_gid,
            0o600,
            frozenset({0, resource_uid}),
        ),
        (
            resource_daemon.service_configuration.high_water_cache_path,
            resource_uid,
            resource_client_gid,
            0o600,
            frozenset({0, resource_uid}),
        ),
        (
            resource_daemon.operation_private_key_path,
            resource_uid,
            resource_client_gid,
            0o400,
            frozenset({0, resource_uid}),
        ),
        (
            resource_daemon.operation_public_key_path,
            resource_uid,
            resource_client_gid,
            0o440,
            frozenset({0, resource_uid}),
        ),
        (
            resource_daemon.root_public_key_path,
            0,
            root_client_gid,
            0o440,
            frozenset({0, root_uid}),
        ),
    ):
        require_metadata(
            path,
            kind="file",
            uid=uid,
            gid=gid,
            mode=mode,
            ancestor_uids=ancestor_uids,
        )
    require_metadata(
        root_service.socket_path,
        kind="socket",
        uid=root_uid,
        gid=root_client_gid,
        mode=0o660,
        ancestor_uids=frozenset({0, root_uid}),
    )
    require_metadata(
        adapter.endpoint,
        kind="socket",
        uid=resource_uid,
        gid=resource_client_gid,
        mode=0o660,
        ancestor_uids=frozenset({0, resource_uid}),
    )

    root_groups = frozenset()
    resource_groups = frozenset({root_client_gid})
    application_groups = frozenset({resource_client_gid})

    def require_access(
        path: Path,
        requested: int,
        *,
        uid: int,
        gid: int,
        supplementary: frozenset[int],
        expected: bool,
        label: str,
    ) -> None:
        try:
            actual = can_access(path, requested, uid, gid, supplementary)
        except (OSError, TypeError, ValueError) as exc:
            raise ResourceAuthorityServiceError("authority access probe failed") from exc
        if type(actual) is not bool or actual is not expected:
            raise ResourceAuthorityServiceError(label)

    for path, requested in (
        (root_runtime, os.W_OK),
        (root_state, os.W_OK),
        (root_daemon.backend_path, os.R_OK | os.W_OK),
        (root_daemon.private_key_path, os.R_OK),
    ):
        require_access(
            path,
            requested,
            uid=root_uid,
            gid=root_gid,
            supplementary=root_groups,
            expected=True,
            label="external root owner capability is unavailable",
        )
    for path, requested in (
        (root_runtime, os.X_OK),
        (root_service.socket_path, os.W_OK),
    ):
        require_access(
            path,
            requested,
            uid=resource_uid,
            gid=resource_gid,
            supplementary=resource_groups,
            expected=True,
            label="resource authority cannot reach external root socket",
        )
    for path, requested, label in (
        (root_runtime, os.W_OK, "resource authority can replace external root socket"),
        (root_state, os.W_OK, "resource authority can write external root state"),
        (root_daemon.backend_path, os.W_OK, "resource authority can write root backend"),
        (root_daemon.private_key_path, os.R_OK, "resource authority can read root private key"),
    ):
        require_access(
            path,
            requested,
            uid=resource_uid,
            gid=resource_gid,
            supplementary=resource_groups,
            expected=False,
            label=label,
        )
    for path, requested in (
        (resource_runtime, os.W_OK),
        (resource_state, os.W_OK),
        (
            resource_daemon.service_configuration.resource_journal_path,
            os.R_OK | os.W_OK,
        ),
        (resource_daemon.operation_private_key_path, os.R_OK),
    ):
        require_access(
            path,
            requested,
            uid=resource_uid,
            gid=resource_gid,
            supplementary=resource_groups,
            expected=True,
            label="resource authority owner capability is unavailable",
        )
    for path, requested in (
        (resource_runtime, os.X_OK),
        (adapter.endpoint, os.W_OK),
    ):
        require_access(
            path,
            requested,
            uid=application_uid,
            gid=application_gid,
            supplementary=application_groups,
            expected=True,
            label="lighthouse cannot reach resource authority socket",
        )
    for path, requested, label in (
        (resource_runtime, os.W_OK, "lighthouse can replace resource authority socket"),
        (root_daemon.private_key_path, os.R_OK, "lighthouse can read root private key"),
        (
            resource_daemon.operation_private_key_path,
            os.R_OK,
            "lighthouse can read resource authority private key",
        ),
        (root_runtime, os.X_OK, "lighthouse can reach external root socket"),
    ):
        require_access(
            path,
            requested,
            uid=application_uid,
            gid=application_gid,
            supplementary=application_groups,
            expected=False,
            label=label,
        )
    return (
        "external-root uid/gid and private state isolated",
        "resource-authority uid/gid and private state isolated",
        "lighthouse restricted to resource socket client group",
    )


class ResourceJournalExternalRootRoleHandler:
    """Maps generic persisted CAS state to legacy-compatible signed root receipts."""

    def __init__(
        self,
        *,
        high_water_authority_id: str,
        signer: OpenSslExternalMonotonicRootSigner,
    ) -> None:
        self.high_water_authority_id = high_water_authority_id.strip()
        self.signer = signer
        if (
            not self.high_water_authority_id
            or type(signer) is not OpenSslExternalMonotonicRootSigner
            or signer.key_purpose != RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
            or RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE not in signer.allowed_namespaces
        ):
            raise ResourceAuthorityServiceError(
                "resource external root role signer binding is invalid"
            )

    def response_json(
        self,
        request: object,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        from rquant.external_monotonic_root import ExternalMonotonicRootRequest

        validated_request = ExternalMonotonicRootRequest.model_validate(request, strict=True)
        if state is None:
            return None
        if (
            state.role != validated_request.role
            or state.root_authority_id != validated_request.root_authority_id
            or state.root_store_id != validated_request.root_store_id
            or state.subject_authority_id != validated_request.subject_authority_id
        ):
            raise ResourceAuthorityServiceError("resource external root state identity conflicts")
        checkpoint = strict_model_validate_canonical_json(
            ResourceJournalHighWaterCheckpoint,
            state.checkpoint_json,
        )
        unsigned_receipt = ResourceJournalAntiRollbackReceipt(
            schema_version=1,
            contract="rquant-resource-journal-anti-rollback-receipt/v1",
            root_authority_id=state.root_authority_id,
            high_water_authority_id=self.high_water_authority_id,
            journal_authority_id=state.subject_authority_id,
            operation_id=state.operation_id,
            previous_checkpoint_hash=state.previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=self.signer.issuer,
            key_id=self.signer.key_id,
            key_purpose=self.signer.key_purpose,
            namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
            signature_algorithm=self.signer.signature_algorithm,
            public_key_fingerprint=self.signer.public_key_fingerprint,
            signature="pending",
        )
        receipt = unsigned_receipt.model_copy(
            update={
                "signature": self.signer.sign(
                    namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                    payload=unsigned_receipt.signing_bytes(),
                )
            }
        )
        unsigned_external = ResourceJournalExternalRootReceipt(
            journal_authority_id=state.subject_authority_id,
            request_kind=validated_request.kind,
            request_hash=validated_request.request_hash,
            challenge_nonce=validated_request.challenge_nonce,
            root_authority_id=state.root_authority_id,
            root_store_id=state.root_store_id,
            receipt=receipt,
            issuer=self.signer.issuer,
            key_id=self.signer.key_id,
            public_key_fingerprint=self.signer.public_key_fingerprint,
            signature="pending",
        )
        external = unsigned_external.model_copy(
            update={
                "signature": self.signer.sign(
                    namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                    payload=unsigned_external.signing_bytes(),
                )
            }
        )
        return canonical_json_bytes(external.model_dump(mode="json")).decode("utf-8")


class ResourceAuthorityUnixService:
    def __init__(self, server: ResourceAuthorityJournalSocketServer) -> None:
        self.server = server
        self.ready = threading.Event()

    def serve_forever(self, *, stop: threading.Event | None = None) -> None:
        stop_event = stop or threading.Event()
        listener = self.server.bind()
        listener.settimeout(0.05)
        self.ready.set()
        try:
            while not stop_event.is_set():
                try:
                    self.server.serve_once(listener)
                except TimeoutError:
                    continue
        finally:
            listener.close()
            self.server.configuration.endpoint.unlink(missing_ok=True)
            self.ready.clear()

    def wake(self) -> None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(os.fspath(self.server.configuration.endpoint))
        except OSError:
            return


def _load_daemon_configuration(
    path: Path,
    model: type[ConfigurationT],
    *,
    trusted_root: Path = Path("/"),
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> ConfigurationT:
    candidate = Path(path)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise ResourceAuthorityServiceError("daemon configuration path is not canonical")
    uid = os.geteuid() if expected_uid is None else expected_uid
    gid = os.getegid() if expected_gid is None else expected_gid
    try:
        payload_bytes = read_secure_regular_file(
            candidate,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, uid, os.geteuid()}),
            expected_uid=uid,
            expected_gid=gid,
            allowed_modes=frozenset({0o400, 0o440, 0o444, 0o600}),
            max_bytes=_MAX_DAEMON_CONFIGURATION_BYTES,
        )
        payload = payload_bytes.decode("utf-8")
        return strict_model_validate_canonical_json(model, payload)
    except ResourceAuthorityServiceError:
        raise
    except AuthorityPathSecurityError as exc:
        raise ResourceAuthorityServiceError("daemon configuration identity is unsafe") from exc
    except (OSError, TypeError, ValueError, UnicodeError) as exc:
        raise ResourceAuthorityServiceError("daemon configuration is unavailable") from exc


def load_external_monotonic_root_daemon_configuration(
    path: Path,
    *,
    trusted_root: Path = Path("/"),
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> ExternalMonotonicRootDaemonConfiguration:
    return _load_daemon_configuration(
        path,
        ExternalMonotonicRootDaemonConfiguration,
        trusted_root=trusted_root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )


def load_resource_authority_daemon_configuration(
    path: Path,
    *,
    trusted_root: Path = Path("/"),
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> ResourceAuthorityDaemonConfiguration:
    return _load_daemon_configuration(
        path,
        ResourceAuthorityDaemonConfiguration,
        trusted_root=trusted_root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )


def compose_external_monotonic_root_daemon(
    configuration: ExternalMonotonicRootDaemonConfiguration,
) -> ExternalMonotonicRootUnixService:
    validated = ExternalMonotonicRootDaemonConfiguration.model_validate(configuration, strict=True)
    signer = OpenSslExternalMonotonicRootSigner(
        private_key_path=validated.private_key_path,
        public_key_path=validated.public_key_path,
        issuer=validated.issuer,
        key_id=validated.key_id,
        key_purpose=validated.key_purpose,
        allowed_namespaces=_RESOURCE_ROOT_SIGNING_NAMESPACES,
    )
    service = validated.service_configuration
    return ExternalMonotonicRootUnixService(
        configuration=service,
        backend=PersistentExternalMonotonicRootBackend(
            validated.backend_path,
            role=service.role,
            authority_id=service.authority_id,
            store_id=service.store_id,
        ),
        handler=ResourceJournalExternalRootRoleHandler(
            high_water_authority_id=validated.high_water_authority_id,
            signer=signer,
        ),
        probe_signer=signer,
    )


def compose_resource_authority_daemon(
    *,
    configuration: ResourceAuthorityDaemonConfiguration,
    policy_provider: Callable[[], AdmissionPolicy],
    snapshot_provider: Callable[[], ResourceSnapshot],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ResourceAuthorityUnixService:
    validated = ResourceAuthorityDaemonConfiguration.model_validate(configuration, strict=True)
    service = validated.service_configuration
    root = service.adapter_configuration.external_root_config
    if root is None:  # pragma: no cover - service config enforces production
        raise ResourceAuthorityServiceError("resource authority external root is missing")
    operation_signer = OpenSslExternalMonotonicRootSigner(
        private_key_path=validated.operation_private_key_path,
        public_key_path=validated.operation_public_key_path,
        issuer=validated.operation_issuer,
        key_id=validated.operation_key_id,
        key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
        allowed_namespaces=_RESOURCE_OPERATION_SIGNING_NAMESPACES,
    )
    operation_verifier = ClosedExternalMonotonicRootVerifier(
        public_key_path=validated.operation_public_key_path,
        issuer=validated.operation_issuer,
        key_id=validated.operation_key_id,
        key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
    )
    root_verifier = ClosedExternalMonotonicRootVerifier(
        public_key_path=validated.root_public_key_path,
        issuer=root.root_issuer,
        key_id=root.root_key_id,
        key_purpose=RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    )
    return compose_resource_authority_service(
        configuration=service,
        operation_signer=operation_signer,
        operation_verifier=operation_verifier,
        root_verifier=root_verifier,
        policy_provider=policy_provider,
        snapshot_provider=snapshot_provider,
        clock=clock,
    )


def compose_resource_authority_service(
    *,
    configuration: ResourceAuthorityServiceConfiguration,
    operation_signer: OpenSslExternalMonotonicRootSigner,
    operation_verifier: ClosedExternalMonotonicRootVerifier,
    root_verifier: ClosedExternalMonotonicRootVerifier,
    policy_provider: Callable[[], AdmissionPolicy],
    snapshot_provider: Callable[[], ResourceSnapshot],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ResourceAuthorityUnixService:
    validated = ResourceAuthorityServiceConfiguration.model_validate(configuration, strict=True)
    if (
        type(operation_signer) is not OpenSslExternalMonotonicRootSigner
        or type(operation_verifier) is not ClosedExternalMonotonicRootVerifier
        or type(root_verifier) is not ClosedExternalMonotonicRootVerifier
        or operation_signer.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
        or operation_verifier.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
        or operation_signer.public_key_fingerprint != operation_verifier.public_key_fingerprint
        or root_verifier.key_purpose != RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
    ):
        raise ResourceAuthorityServiceError(
            "resource authority production signing capabilities are not closed"
        )
    root_config = validated.adapter_configuration.external_root_config
    if root_config is None:  # pragma: no cover - config model enforces it
        raise ResourceAuthorityServiceError("resource authority external root is missing")
    if (
        root_verifier.issuer,
        root_verifier.key_id,
        root_verifier.public_key_fingerprint,
    ) != (
        root_config.root_issuer,
        root_config.root_key_id,
        root_config.root_public_key_fingerprint,
    ):
        raise ResourceAuthorityServiceError("resource authority root verifier identity conflicts")
    try:
        for state_path in (
            validated.high_water_cache_path,
            validated.resource_journal_path,
        ):
            secure_create_regular_file(
                state_path,
                allowed_ancestor_uids=frozenset({0, os.geteuid()}),
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
                expected_mode=0o600,
            )
    except AuthorityPathSecurityError as exc:
        raise ResourceAuthorityServiceError(
            "resource authority state path or ancestor is unsafe"
        ) from exc
    root_client = UnixSocketExternalMonotonicRootClient(validated.external_root_manifest)
    external_root = ExternalResourceJournalMonotonicRootAdapter(
        config=root_config,
        client=root_client,
        root_verifiers=(root_verifier,),
    )
    high_water = SQLiteResourceJournalHighWaterAuthority(
        validated.high_water_cache_path,
        authority_id=validated.adapter_configuration.high_water_authority_id or "",
        trusted_role_inventory=validated.trusted_role_inventory.to_inventory(),
        journal_verifiers=(operation_verifier,),
        trusted_journal_issuer=validated.trusted_journal_issuer,
        anti_rollback_root=external_root,
        root_verifiers=(root_verifier,),
        trusted_root_issuer=root_verifier.issuer,
        mode="production",
    )
    operation_keyring = ClosedResourceOperationKeyring(
        verifiers=(operation_verifier,),
        trusted_issuer=validated.trusted_journal_issuer,
        trusted_role_inventory=validated.trusted_role_inventory.to_inventory(),
    )
    authority = SQLiteResourceAdmissionAuthority(
        validated.resource_journal_path,
        authority_id=validated.adapter_configuration.authority_id,
        signer=operation_signer,
        keyring=operation_keyring,
        high_water_authority=high_water,
        mode="production",
        clock=clock,
    )
    try:
        for state_path in (
            validated.high_water_cache_path,
            validated.resource_journal_path,
        ):
            secure_path_metadata(
                state_path,
                allowed_ancestor_uids=frozenset({0, os.geteuid()}),
                kind="file",
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
                expected_mode=0o600,
            )
    except AuthorityPathSecurityError as exc:
        raise ResourceAuthorityServiceError(
            "resource authority state binding changed during initialization"
        ) from exc
    server = compose_production_resource_authority_socket_server(
        configuration=validated.adapter_configuration,
        authority=authority,
        policy_provider=policy_provider,
        snapshot_provider=snapshot_provider,
        external_root_client=root_client,
        external_root_verifiers=(root_verifier,),
    )
    return ResourceAuthorityUnixService(server)


def probe_resource_authority_service(
    configuration: ResourceAuthorityAdapterConfig,
) -> ResourceAuthorityServiceProbe:
    client = ResourceAuthorityJournalClient(configuration)
    operation_seed = canonical_sha256(
        {
            "contract": "rquant-resource-authority-service-probe/v1",
            "authority_id": configuration.authority_id,
        }
    )
    capabilities = client.probe(operation_id=operation_seed)
    client.policy(operation_id=canonical_sha256({"probe": operation_seed, "kind": "policy"}))
    client.snapshot(operation_id=canonical_sha256({"probe": operation_seed, "kind": "snapshot"}))
    return ResourceAuthorityServiceProbe(
        identity=ResourceAuthorityAdapterIdentity(
            mode=configuration.mode,
            authority_id=configuration.authority_id,
            high_water_authority_id=configuration.high_water_authority_id,
            external_root_config=configuration.external_root_config,
            trusted_role_inventory_hash=configuration.trusted_role_inventory_hash,
        ),
        capabilities=capabilities,
    )


__all__ = [
    "EXTERNAL_ROOT_CLIENT_GROUP",
    "EXTERNAL_ROOT_ENVIRONMENT_KEYS",
    "EXTERNAL_ROOT_ENVIRONMENT_PATH",
    "EXTERNAL_ROOT_SERVICE_USER",
    "ExternalMonotonicRootDaemonConfiguration",
    "ResourceAuthorityDaemonConfiguration",
    "ResourceAuthorityServiceConfiguration",
    "ResourceAuthorityServiceError",
    "ResourceAuthorityServiceProbe",
    "ResourceAuthorityUnixService",
    "ResourceJournalExternalRootRoleHandler",
    "RESOURCE_AUTHORITY_APPLICATION_USER",
    "RESOURCE_AUTHORITY_CLIENT_GROUP",
    "RESOURCE_AUTHORITY_ENVIRONMENT_KEYS",
    "RESOURCE_AUTHORITY_ENVIRONMENT_PATH",
    "RESOURCE_AUTHORITY_SERVICE_USER",
    "TrustedRoleInventoryConfiguration",
    "compose_external_monotonic_root_daemon",
    "compose_resource_authority_daemon",
    "compose_resource_authority_service",
    "load_external_monotonic_root_daemon_configuration",
    "load_closed_authority_environment",
    "load_resource_authority_daemon_configuration",
    "probe_resource_authority_service",
    "verify_authority_os_isolation",
]
