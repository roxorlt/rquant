from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import rquant.source_broker_v2_runtime as runtime_module
from rquant.authority_path_security import SecurePathMetadata
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2AuthorityRuntime,
    SourceBrokerV2RootRole,
    SourceBrokerV2RuntimeSecurityError,
    source_broker_v2_default_runtime,
)


def _identity(
    role: object,
    *,
    uid: int,
    gid: int,
) -> object:
    return runtime_module.SourceBrokerV2ProcessIdentity(role=role, uid=uid, gid=gid)


def _identities() -> object:
    role = runtime_module.SourceBrokerV2ProcessRole
    return runtime_module.SourceBrokerV2IdentityMatrix(
        current_claim=_identity(
            role.CURRENT_CLAIM_AUTHORITY,
            uid=51_001,
            gid=61_001,
        ),
        source_quota=_identity(
            role.SOURCE_QUOTA_AUTHORITY,
            uid=51_002,
            gid=61_002,
        ),
        replay_lineage=_identity(
            role.REPLAY_LINEAGE_AUTHORITY,
            uid=51_003,
            gid=61_003,
        ),
        current_claim_root=_identity(
            role.CURRENT_CLAIM_ROOT_SERVICE,
            uid=51_004,
            gid=61_001,
        ),
        source_quota_root=_identity(
            role.SOURCE_QUOTA_ROOT_SERVICE,
            uid=51_005,
            gid=61_002,
        ),
        replay_lineage_root=_identity(
            role.REPLAY_LINEAGE_ROOT_SERVICE,
            uid=51_006,
            gid=61_003,
        ),
        source_daemon=_identity(
            role.SOURCE_DAEMON,
            uid=51_007,
            gid=61_004,
        ),
        scheduler_client=_identity(
            role.SCHEDULER_SOURCE_CLIENT,
            uid=51_008,
            gid=61_004,
        ),
    )


def _runtime(root: Path) -> SourceBrokerV2AuthorityRuntime:
    return source_broker_v2_default_runtime(root=root, identities=_identities())


def test_runtime_exposes_explicit_role_identity_contract() -> None:
    assert hasattr(runtime_module, "SourceBrokerV2ProcessIdentity")
    assert hasattr(runtime_module, "SourceBrokerV2IdentityMatrix")
    assert hasattr(runtime_module, "SourceBrokerV2ProcessRole")


def test_runtime_is_strict_and_separates_every_private_role_layout(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)

    with pytest.raises(ValidationError, match="extra_forbidden"):
        SourceBrokerV2AuthorityRuntime.model_validate(
            {**runtime.model_dump(mode="python"), "fallback": "memory"},
            strict=True,
        )

    private_layouts = runtime.private_layouts
    assert len({layout.identity.uid for layout in private_layouts}) == len(private_layouts)
    assert len({layout.state_directory for layout in private_layouts}) == len(private_layouts)
    assert len({layout.key_directory for layout in private_layouts}) == len(private_layouts)
    assert all(layout.state_path.parent == layout.state_directory for layout in private_layouts)
    assert all(layout.private_key_path.parent == layout.key_directory for layout in private_layouts)
    assert all(layout.state_directory_mode == 0o700 for layout in private_layouts)
    assert all(layout.key_directory_mode == 0o700 for layout in private_layouts)
    assert all(layout.private_key_mode == 0o600 for layout in private_layouts)

    assert runtime.client_public_key_paths == (
        runtime.scheduler_client.source_current_public_key_path,
        runtime.scheduler_client.source_next_public_key_path,
    )
    assert all(
        path.parent == runtime.scheduler_client.key_directory
        for path in runtime.client_public_key_paths
    )
    assert runtime.source_daemon.private_key_path not in runtime.client_public_key_paths
    assert runtime.source_daemon.next_private_key_path not in runtime.client_public_key_paths
    assert runtime.source_daemon.next_public_key_path not in runtime.client_public_key_paths

    authority_services = (
        runtime.current_claim,
        runtime.source_quota,
        runtime.replay_lineage,
    )
    assert len({authority.run_directory for authority in authority_services}) == 3
    assert len({authority.socket_path for authority in authority_services}) == 3
    assert all(
        authority.socket_path.parent == authority.run_directory for authority in authority_services
    )
    assert runtime.current_claim.manifest_public_key_path is not None
    assert (
        runtime.current_claim.manifest_public_key_path.parent == runtime.current_claim.key_directory
    )
    assert runtime.source_quota.manifest_public_key_path is None
    assert runtime.replay_lineage.manifest_public_key_path is None
    assert len(runtime.replay_lineage.separation_state_paths) == 2
    assert all(
        path.parent == runtime.replay_lineage.state_directory
        for path in runtime.replay_lineage.separation_state_paths
    )


def test_production_identity_matrix_rejects_shared_uid_or_unrelated_group() -> None:
    identities = _identities()

    with pytest.raises(ValidationError, match="UID.*independent|owner.*reused"):
        runtime_module.SourceBrokerV2IdentityMatrix.model_validate(
            identities.model_dump(mode="python")
            | {
                "source_quota": identities.source_quota.model_copy(
                    update={"uid": identities.current_claim.uid}
                )
            },
            strict=True,
        )

    with pytest.raises(ValidationError, match="socket group|GID"):
        runtime_module.SourceBrokerV2IdentityMatrix.model_validate(
            identities.model_dump(mode="python")
            | {
                "source_quota_root": identities.source_quota_root.model_copy(
                    update={"gid": identities.current_claim.gid}
                )
            },
            strict=True,
        )


def test_root_and_source_daemon_policies_pin_exact_consumer_peer() -> None:
    runtime = _runtime(Path("/private/tmp/rquant-v2-runtime-policy"))

    for role, consumer in (
        (SourceBrokerV2RootRole.CURRENT_CLAIM, runtime.identities.current_claim),
        (SourceBrokerV2RootRole.SOURCE_QUOTA, runtime.identities.source_quota),
        (SourceBrokerV2RootRole.REPLAY_LINEAGE, runtime.identities.replay_lineage),
    ):
        policy = runtime.root(role).unix_policy
        assert policy.allowed_peer_uid == consumer.uid
        assert policy.allowed_peer_gid == consumer.gid
        assert policy.allows_peer(uid=consumer.uid, gid=consumer.gid)
        assert not policy.allows_peer(uid=consumer.uid + 1, gid=consumer.gid)
        assert not policy.allows_peer(uid=consumer.uid, gid=consumer.gid + 1)
        assert policy.socket_mode == 0o660
        assert policy.run_directory_mode == 0o750

    source_policy = runtime.source_daemon.unix_policy(runtime.identities.scheduler_client)
    assert source_policy.allowed_peer_uid == runtime.identities.scheduler_client.uid
    assert source_policy.allowed_peer_gid == runtime.identities.scheduler_client.gid
    assert source_policy.allows_peer(
        uid=runtime.identities.scheduler_client.uid,
        gid=runtime.identities.scheduler_client.gid,
    )
    assert not source_policy.allows_peer(
        uid=runtime.identities.current_claim.uid,
        gid=runtime.identities.current_claim.gid,
    )


def test_filesystem_validation_uses_each_roles_owner_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    observed: dict[Path, tuple[int, int, int, str]] = {}

    for directory in runtime.protected_directories:
        directory.path.mkdir(parents=True, mode=directory.mode, exist_ok=True)
        directory.path.chmod(directory.mode)
    for file in runtime.protected_files:
        file.path.touch(mode=file.mode, exist_ok=True)
        file.path.chmod(file.mode)

    def metadata(
        path: Path,
        *,
        trusted_root: Path = Path("/"),
        allowed_ancestor_uids: frozenset[int] | None = None,
        kind: str,
        expected_uid: int,
        expected_gid: int,
        expected_mode: int,
    ) -> SecurePathMetadata:
        del trusted_root, allowed_ancestor_uids
        observed[path] = (expected_uid, expected_gid, expected_mode, kind)
        return SecurePathMetadata(
            uid=expected_uid,
            gid=expected_gid,
            mode=expected_mode,
            device=1,
            inode=len(observed),
            size=path.stat().st_size,
        )

    monkeypatch.setattr(runtime_module, "secure_path_metadata", metadata)
    runtime.validate_filesystem(require_key_files=True, require_state_files=True)

    assert observed[runtime.current_claim.private_key_path][:2] == (
        runtime.identities.current_claim.uid,
        runtime.identities.current_claim.gid,
    )
    assert observed[runtime.source_quota.private_key_path][:2] == (
        runtime.identities.source_quota.uid,
        runtime.identities.source_quota.gid,
    )
    assert observed[runtime.source_daemon.private_key_path][:2] == (
        runtime.identities.source_daemon.uid,
        runtime.identities.source_daemon.gid,
    )
    assert observed[runtime.scheduler_client.source_current_public_key_path][:2] == (
        runtime.identities.scheduler_client.uid,
        runtime.identities.scheduler_client.gid,
    )


def test_layout_rejects_reused_parent_or_role_owner(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    shared_directory = runtime.model_copy(
        update={
            "source_quota": runtime.source_quota.model_copy(
                update={"key_directory": runtime.current_claim.key_directory}
            )
        }
    )
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="directories.*independent"):
        shared_directory.validate_layout()

    wrong_owner = runtime.model_copy(
        update={
            "replay_lineage": runtime.replay_lineage.model_copy(
                update={"identity": runtime.current_claim.identity}
            )
        }
    )
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="identity|owner"):
        wrong_owner.validate_layout()


def test_layout_rejects_root_key_copy_rebinding_root_order_and_key_id_changes(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    current_root = runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM)
    rebound_root = current_root.model_copy(
        update={
            "consumer_public_key_path": runtime.replay_lineage.root_public_key_path,
        }
    )
    rebound_runtime = runtime.model_copy(
        update={
            "roots": tuple(rebound_root if root is current_root else root for root in runtime.roots)
        }
    )
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="verification key|binding"):
        rebound_runtime.validate_layout()

    reordered = runtime.model_copy(update={"roots": tuple(reversed(runtime.roots))})
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="root role order|binding"):
        reordered.validate_layout()

    changed_key_id = runtime.model_copy(
        update={"source_authority_next_key_id": "source-broker-v2-source-rebound"}
    )
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="key.*binding"):
        changed_key_id.validate_layout()


def test_production_identity_matrix_rejects_root_identity() -> None:
    identities = _identities()
    with pytest.raises(ValidationError, match="greater than 0"):
        runtime_module.SourceBrokerV2IdentityMatrix.model_validate(
            identities.model_dump(mode="python")
            | {
                "current_claim": identities.current_claim.model_copy(update={"uid": 0}),
            },
            strict=True,
        )


def test_linux_system_user_contract_lists_every_real_role() -> None:
    role = runtime_module.SourceBrokerV2ProcessRole
    assert runtime_module.SOURCE_BROKER_V2_LINUX_SYSTEM_USERS == {
        role.CURRENT_CLAIM_AUTHORITY: "rquant-current-claim",
        role.SOURCE_QUOTA_AUTHORITY: "rquant-source-quota",
        role.REPLAY_LINEAGE_AUTHORITY: "rquant-replay-lineage",
        role.CURRENT_CLAIM_ROOT_SERVICE: "rquant-current-root",
        role.SOURCE_QUOTA_ROOT_SERVICE: "rquant-quota-root",
        role.REPLAY_LINEAGE_ROOT_SERVICE: "rquant-lineage-root",
        role.SOURCE_DAEMON: "rquant-source-daemon",
        role.SCHEDULER_SOURCE_CLIENT: "rquant-scheduler",
    }


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux system-user gate")
def test_linux_system_user_gate_requires_exact_uid_gid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identities = _identities()
    by_name = {
        runtime_module.SOURCE_BROKER_V2_LINUX_SYSTEM_USERS[identity.role]: SimpleNamespace(
            pw_uid=identity.uid,
            pw_gid=identity.gid,
        )
        for identity in identities.all
    }
    monkeypatch.setattr(runtime_module.pwd, "getpwnam", by_name.__getitem__)
    runtime_module.require_source_broker_v2_linux_system_users(identities)

    wrong = dict(by_name)
    name = runtime_module.SOURCE_BROKER_V2_LINUX_SYSTEM_USERS[
        runtime_module.SourceBrokerV2ProcessRole.SOURCE_DAEMON
    ]
    wrong[name] = SimpleNamespace(pw_uid=os.getuid(), pw_gid=os.getgid())
    monkeypatch.setattr(runtime_module.pwd, "getpwnam", wrong.__getitem__)
    with pytest.raises(SourceBrokerV2RuntimeSecurityError, match="system user.*identity"):
        runtime_module.require_source_broker_v2_linux_system_users(identities)
