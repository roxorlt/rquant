"""The rollout plans a build before #228 staged for a release that changed no schema.

Until package AJ, `install_runtime_deployment_profile` compared the declarations'
`schema_fingerprint`, which carries `producer_commit`, so every install over a predecessor
prepared one plan per policy channel — sixteen on the production profile — although no
channel's shape had moved. The installer no longer does that (a generation whose channels
keep their shape gets no plan), but the production host still carries every plan those
installs left behind: 208 by 2026-09-25, all past their window.

Tests that are about what happens *around* such plans — admission under a read-only rollout
root (#227), the installer's acknowledgement, a superseded generation's plans — use this to
put them there the way the old installer did: the same loop, the same policy parameters,
through the same `prepare_runtime_schema_rollout`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from rquant.runtime_deployment_bundle import prepare_runtime_schema_rollout
from rquant.runtime_deployment_profile import RuntimeDeploymentProfile


def stage_pre_228_rollout_plans(
    runtime_root: Path,
    *,
    profile: RuntimeDeploymentProfile,
    previous_generation_id: str,
    target_generation_id: str,
    started_at: datetime,
) -> tuple[str, ...]:
    """Prepare one plan per policy channel, exactly as the pre-#228 installer did."""

    plan_ids: list[str] = []
    for policy in profile.schema_rollout_policies:
        authority = prepare_runtime_schema_rollout(
            runtime_root,
            previous_generation_id=previous_generation_id,
            target_generation_id=target_generation_id,
            channel_id=policy.channel_id,
            started_at=started_at,
            deadline=started_at + timedelta(seconds=policy.stage_timeout_seconds),
            consumer_ack_max_age_seconds=policy.consumer_ack_max_age_seconds,
            retire_observation_seconds=policy.retire_observation_seconds,
        )
        plan_ids.append(authority.plan_id)
    return tuple(sorted(plan_ids))


__all__ = ["stage_pre_228_rollout_plans"]
