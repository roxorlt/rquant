"""Pre-checkout R07 differential-gate evidence for the exact-target deployer.

The deployer reads the installed and target policies from resolved Git objects, decides the
Release A/B bootstrap transition from those two policies alone, and — whenever the target policy
is ``enforced`` — downloads, caches, and re-verifies the fixed post-merge ``push main`` evidence
through the one private verifier before any checkout or service mutation happens.

Nothing here rebuilds an artifact, accepts a branch name, adds a production API, or creates an
R07 writer, and no failure is downgraded to a warning.
"""

from __future__ import annotations

import io
import json
import os
import stat
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Literal, Protocol, Self, cast
from urllib.parse import urlencode

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from rquant.signal_family_differential_gate import (
    POLICY_RELATIVE_PATH,
    R07_EVIDENCE_CACHE_DIR,
    BootstrapPredecessorV1,
    R07DrGateEvidenceWireV1,
    R07PolicyV1,
    verify_channel_binding,
    verify_policy_completeness,
    verify_wire,
)
from rquant.strict_json import strict_canonical_json_loads

_SHA1 = r"^[0-9a-f]{40}$"
_SHA256 = r"^[0-9a-f]{64}$"
GITHUB_API_ROOT = "https://api.github.com"
EVIDENCE_REPOSITORY = "roxorlt/rquant"
EVIDENCE_WORKFLOW_FILE = "ci.yml"
EVIDENCE_WORKFLOW_PATH = ".github/workflows/ci.yml"
EVIDENCE_ARTIFACT_JSON_PATH = "r07-dr-gate/evidence-v1.json"
EVIDENCE_TOKEN_VARIABLE = "RQUANT_GITHUB_EVIDENCE_TOKEN"
LINUX_PRODUCTION_EVIDENCE_CACHE_DIR = Path(R07_EVIDENCE_CACHE_DIR)
DEFAULT_EVIDENCE_VERIFIER = verify_wire
_DOWNLOAD_BUDGET_SECONDS = 600.0
_REQUEST_TIMEOUT_SECONDS = 60.0
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_CACHE_FILE_MODE = 0o644
_CACHE_DIR_MODE = 0o755

DeploymentMode = Literal["disabled_for_bootstrap", "enforced"]
PolicyRole = Literal["installed", "target"]


class R07EvidenceError(RuntimeError):
    """The R07 deployment evidence channel refused the requested target."""


class CommandRunner(Protocol):
    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]: ...


class EvidenceTransport(Protocol):
    def get(self, url: str, *, token: str, accept: str) -> bytes: ...


class TokenProvider(Protocol):
    def token(self) -> str: ...


class EvidenceVerifier(Protocol):
    def __call__(
        self,
        repo: Path,
        policy: R07PolicyV1,
        wire: R07DrGateEvidenceWireV1,
    ) -> object: ...


class _StrictModelMixin:
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=False,
    )


class _ResolvedPolicyState(_StrictModelMixin, BaseModel):
    """What one resolved Git commit declares about the R07 deployment channel."""

    commit_sha: StrictStr = Field(pattern=_SHA1)
    tree_sha: StrictStr = Field(pattern=_SHA1)
    deployment_mode: DeploymentMode | None
    bootstrap_predecessor: BootstrapPredecessorV1 | None
    policy_digest: StrictStr | None = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_presence(self) -> Self:
        if self.deployment_mode is None and (
            self.policy_digest is not None or self.bootstrap_predecessor is not None
        ):
            raise ValueError("an absent R07 policy declares no digest or predecessor")
        if self.deployment_mode is not None and self.policy_digest is None:
            raise ValueError("a present R07 policy must carry its digest")
        return self

    @property
    def mode_label(self) -> str:
        return self.deployment_mode or "absent"

    @property
    def pair(self) -> BootstrapPredecessorV1:
        return BootstrapPredecessorV1(commit_sha=self.commit_sha, tree_sha=self.tree_sha)


class ResolvedRunIdentityV1(_StrictModelMixin, BaseModel):
    """The one push-main run identity and attempt the fixed channel resolved for a target."""

    workflow_run_id: StrictInt = Field(gt=0)
    run_attempt: StrictInt = Field(gt=0)


class InstalledPolicyState(_ResolvedPolicyState):
    """The policy of the commit currently checked out in production."""


class TargetPolicyState(_ResolvedPolicyState):
    """The policy of the exact deployment target, read before checkout."""


class R07DeployDecision(_StrictModelMixin, BaseModel):
    allowed: bool
    gate: Literal["bootstrap_disabled", "enforced", "rejected"]
    reason: StrictStr
    requires_evidence: bool
    installed_mode: StrictStr
    target_mode: StrictStr
    installed_commit_sha: StrictStr = Field(pattern=_SHA1)
    installed_tree_sha: StrictStr = Field(pattern=_SHA1)
    target_commit_sha: StrictStr = Field(pattern=_SHA1)
    target_tree_sha: StrictStr = Field(pattern=_SHA1)

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.allowed == (self.gate == "rejected"):
            raise ValueError("an R07 decision is allowed exactly when it is not rejected")
        if self.requires_evidence and self.gate != "enforced":
            raise ValueError("only an enforced R07 target consumes channel evidence")
        return self

    def audit_fields(self) -> dict[str, str]:
        return {
            "r07_gate": self.gate,
            "r07_reason": self.reason,
            "r07_installed_mode": self.installed_mode,
            "r07_target_mode": self.target_mode,
            "r07_installed_commit_sha": self.installed_commit_sha,
            "r07_installed_tree_sha": self.installed_tree_sha,
            "r07_target_commit_sha": self.target_commit_sha,
            "r07_target_tree_sha": self.target_tree_sha,
        }


def _decision(
    installed: InstalledPolicyState,
    target: TargetPolicyState,
    *,
    allowed: bool,
    gate: Literal["bootstrap_disabled", "enforced", "rejected"],
    reason: str,
    requires_evidence: bool = False,
) -> R07DeployDecision:
    return R07DeployDecision(
        allowed=allowed,
        gate=gate,
        reason=reason,
        requires_evidence=requires_evidence,
        installed_mode=installed.mode_label,
        target_mode=target.mode_label,
        installed_commit_sha=installed.commit_sha,
        installed_tree_sha=installed.tree_sha,
        target_commit_sha=target.commit_sha,
        target_tree_sha=target.tree_sha,
    )


def decide_r07_deployment(
    installed: InstalledPolicyState,
    target: TargetPolicyState,
) -> R07DeployDecision:
    """Apply the fixed Release A/B transition table to two resolved policies."""

    if type(installed) is not InstalledPolicyState or type(target) is not TargetPolicyState:
        raise TypeError("the R07 decision table requires exact installed and target states")
    if target.deployment_mode is None:
        return _decision(
            installed,
            target,
            allowed=False,
            gate="rejected",
            reason="target declares no R07 policy; a pre-R07 target is never deployable",
        )
    if target.deployment_mode == "disabled_for_bootstrap":
        if installed.deployment_mode is None:
            return _decision(
                installed,
                target,
                allowed=True,
                gate="bootstrap_disabled",
                reason="Release A bootstrap install from a pre-R07 checkout",
            )
        return _decision(
            installed,
            target,
            allowed=False,
            gate="rejected",
            reason=(
                "a bootstrap-disabled target is rejected once an R07 policy is installed; "
                "the next target must be enforced"
            ),
        )
    if installed.deployment_mode is None:
        return _decision(
            installed,
            target,
            allowed=False,
            gate="rejected",
            reason=(
                "an enforced target cannot name an installed bootstrap predecessor "
                "from a pre-R07 checkout"
            ),
        )
    if installed.deployment_mode == "disabled_for_bootstrap":
        if target.bootstrap_predecessor != installed.pair:
            return _decision(
                installed,
                target,
                allowed=False,
                gate="rejected",
                reason=(
                    "enforced target bootstrap predecessor does not name the installed "
                    "commit and tree pair"
                ),
            )
        return _decision(
            installed,
            target,
            allowed=True,
            gate="enforced",
            reason="Release B enforced transition from the installed bootstrap pair",
            requires_evidence=True,
        )
    return _decision(
        installed,
        target,
        allowed=True,
        gate="enforced",
        reason="enforced target after Release B",
        requires_evidence=True,
    )


def parse_policy_blob(raw: bytes) -> R07PolicyV1:
    """Validate one canonical policy Git blob exactly as ``load_policy`` validates a file."""

    strict_canonical_json_loads(raw)
    policy = R07PolicyV1.model_validate_json(raw)
    completeness = verify_policy_completeness(policy)
    if not completeness.passed:
        raise R07EvidenceError("incomplete R07 policy: " + "; ".join(completeness.reasons))
    return policy


def _git_stdout(
    runner: CommandRunner,
    git_path: Path,
    arguments: list[str],
    *,
    label: str,
) -> str:
    completed = runner.run([str(git_path), *arguments], check=False)
    if completed.returncode != 0:
        raise R07EvidenceError(f"{label} could not be resolved from the Git object store")
    return completed.stdout


def resolve_tree_sha(
    *,
    runner: CommandRunner,
    git_path: Path,
    commit_sha: str,
) -> str:
    tree = _git_stdout(
        runner,
        git_path,
        ["rev-parse", "--verify", f"{commit_sha}^{{tree}}"],
        label="target tree",
    ).strip()
    if len(tree) != 40 or any(char not in "0123456789abcdef" for char in tree):
        raise R07EvidenceError("resolved tree is not a lowercase 40-hex Git object")
    return tree


def read_policy_blob(
    *,
    runner: CommandRunner,
    git_path: Path,
    commit_sha: str,
) -> bytes | None:
    """Read the policy fixture from a resolved Git object, never from the working tree."""

    listing = _git_stdout(
        runner,
        git_path,
        ["ls-tree", "--full-tree", commit_sha, "--", POLICY_RELATIVE_PATH],
        label="policy tree entry",
    )
    if not listing.strip():
        return None
    entries = [line for line in listing.splitlines() if line.strip()]
    if len(entries) != 1:
        raise R07EvidenceError("policy tree entry is ambiguous")
    metadata, _, path = entries[0].partition("\t")
    fields = metadata.split()
    if len(fields) != 3 or path != POLICY_RELATIVE_PATH:
        raise R07EvidenceError("policy tree entry is not the exact declared path")
    mode, object_type, object_id = fields
    if mode != "100644" or object_type != "blob":
        raise R07EvidenceError("policy tree entry is not a regular blob")
    raw = _git_stdout(
        runner,
        git_path,
        ["cat-file", "blob", object_id],
        label="policy blob",
    )
    return raw.encode("utf-8")


def read_policy_state(
    *,
    runner: CommandRunner,
    git_path: Path,
    commit_sha: str,
    role: PolicyRole,
) -> tuple[InstalledPolicyState | TargetPolicyState, R07PolicyV1 | None]:
    """Resolve one commit's tree and declared R07 deployment channel."""

    model = InstalledPolicyState if role == "installed" else TargetPolicyState
    tree_sha = resolve_tree_sha(runner=runner, git_path=git_path, commit_sha=commit_sha)
    raw = read_policy_blob(runner=runner, git_path=git_path, commit_sha=commit_sha)
    if raw is None:
        return (
            model(
                commit_sha=commit_sha,
                tree_sha=tree_sha,
                deployment_mode=None,
                bootstrap_predecessor=None,
                policy_digest=None,
            ),
            None,
        )
    try:
        policy = parse_policy_blob(raw)
    except (ValidationError, ValueError) as exc:
        raise R07EvidenceError(f"policy Git object failed strict validation: {exc}") from exc
    channel = policy.evidence_channel
    return (
        model(
            commit_sha=commit_sha,
            tree_sha=tree_sha,
            deployment_mode=channel.deployment_mode,
            bootstrap_predecessor=channel.bootstrap_predecessor,
            policy_digest=policy.policy_digest,
        ),
        policy,
    )


def cache_entry_path(cache_dir: Path, commit_sha: str) -> Path:
    if len(commit_sha) != 40 or any(char not in "0123456789abcdef" for char in commit_sha):
        raise R07EvidenceError("evidence cache entries are named by a lowercase 40-hex commit")
    return Path(cache_dir) / f"{commit_sha}.json"


def write_cached_evidence(*, cache_dir: Path, commit_sha: str, payload: bytes) -> Path:
    """Write verified evidence bytes into the retained server cache atomically."""

    target = cache_entry_path(cache_dir, commit_sha)
    directory = Path(cache_dir)
    directory.mkdir(parents=True, mode=_CACHE_DIR_MODE, exist_ok=True)
    os.chmod(directory, _CACHE_DIR_MODE)
    temporary = directory / f".{commit_sha}.json.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
        _CACHE_FILE_MODE,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.chmod(temporary, _CACHE_FILE_MODE)
    os.replace(temporary, target)
    directory_descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return target


def _read_cache_bytes(path: Path) -> bytes | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise R07EvidenceError(
            "evidence cache entry is a symlink or is otherwise unreadable"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise R07EvidenceError("evidence cache entry is not a regular file")
        if info.st_size > _MAX_RESPONSE_BYTES:
            raise R07EvidenceError("evidence cache entry is implausibly large")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            return handle.read()
    finally:
        os.close(descriptor)


def bind_evidence_wire(
    raw: bytes,
    *,
    commit_sha: str,
    tree_sha: str,
    policy: R07PolicyV1,
    run_identity: ResolvedRunIdentityV1 | None = None,
) -> R07DrGateEvidenceWireV1:
    """Parse canonical evidence bytes and bind them to the exact target, policy, and run.

    ``run_identity`` is the pair the fixed channel just resolved from the GitHub API. It is
    supplied on the download path, which is the only path that can observe it; a retained cache
    entry was bound to its own resolved pair before it was written, and the artifact it came from
    is allowed to have expired by then.
    """

    try:
        wire = R07DrGateEvidenceWireV1.from_canonical_json(raw)
    except (ValidationError, ValueError) as exc:
        raise R07EvidenceError(f"evidence is not strict canonical wire JSON: {exc}") from exc
    if wire.candidate_commit_sha != commit_sha:
        raise R07EvidenceError("evidence is bound to another candidate commit")
    if wire.candidate_tree_sha != tree_sha:
        raise R07EvidenceError("evidence is bound to another candidate tree")
    if wire.artifact_name != f"r07-dr-gate-{commit_sha}":
        raise R07EvidenceError("evidence artifact name is not bound to the target commit")
    if wire.policy_digest != policy.policy_digest:
        raise R07EvidenceError("evidence policy digest is not the target policy digest")
    if run_identity is not None and (wire.workflow_run_id, wire.run_attempt) != (
        run_identity.workflow_run_id,
        run_identity.run_attempt,
    ):
        raise R07EvidenceError(
            "evidence does not name the resolved push main run identity and attempt"
        )
    try:
        verify_channel_binding(policy, wire)
    except (TypeError, ValueError) as exc:
        raise R07EvidenceError(f"evidence channel metadata mismatch: {exc}") from exc
    return wire


def read_cached_evidence(
    *,
    cache_dir: Path,
    commit_sha: str,
    tree_sha: str,
    policy: R07PolicyV1,
) -> R07DrGateEvidenceWireV1 | None:
    """Open the retained cache entry read-only and reject anything but exact bound evidence."""

    raw = _read_cache_bytes(cache_entry_path(cache_dir, commit_sha))
    if raw is None:
        return None
    return bind_evidence_wire(raw, commit_sha=commit_sha, tree_sha=tree_sha, policy=policy)


def workflow_runs_url(commit_sha: str) -> str:
    query = urlencode(
        {
            "event": "push",
            "branch": "main",
            "head_sha": commit_sha,
            "status": "completed",
            "per_page": "100",
        }
    )
    return (
        f"{GITHUB_API_ROOT}/repos/{EVIDENCE_REPOSITORY}/actions/workflows/"
        f"{EVIDENCE_WORKFLOW_FILE}/runs?{query}"
    )


def run_artifacts_url(workflow_run_id: int) -> str:
    return (
        f"{GITHUB_API_ROOT}/repos/{EVIDENCE_REPOSITORY}/actions/runs/"
        f"{workflow_run_id}/artifacts?per_page=100"
    )


class UrllibEvidenceTransport:
    """Stdlib-only HTTPS reader; the deployer runs under an isolated ``-I -S`` bootstrap."""

    def __init__(self, *, timeout_seconds: float = _REQUEST_TIMEOUT_SECONDS) -> None:
        self._timeout_seconds = timeout_seconds

    def get(self, url: str, *, token: str, accept: str) -> bytes:
        if not url.startswith(f"{GITHUB_API_ROOT}/"):
            raise R07EvidenceError("the evidence channel reads only the fixed GitHub API host")
        request = urllib.request.Request(url, method="GET")  # noqa: S310 - fixed https host
        request.add_header("Accept", accept)
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        request.add_header("User-Agent", "rquant-production-deploy")
        try:
            with urllib.request.urlopen(  # noqa: S310 - fixed https host
                request,
                timeout=self._timeout_seconds,
            ) as response:
                payload = response.read(_MAX_RESPONSE_BYTES + 1)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise R07EvidenceError(f"evidence channel request failed: {type(exc).__name__}") from (
                exc
            )
        if len(payload) > _MAX_RESPONSE_BYTES:
            raise R07EvidenceError("evidence channel response is implausibly large")
        return payload


class EnvironmentTokenProvider:
    """Read the deployment-only GitHub token from the server environment."""

    def token(self) -> str:
        value = os.environ.get(EVIDENCE_TOKEN_VARIABLE, "")
        if not value:
            raise R07EvidenceError(f"{EVIDENCE_TOKEN_VARIABLE} is not configured on this host")
        return value


class _DownloadDeadline:
    def __init__(self, clock: Callable[[], float], budget_seconds: float) -> None:
        self._clock = clock
        self._deadline = clock() + budget_seconds

    def check(self) -> None:
        if self._clock() >= self._deadline:
            raise R07EvidenceError("evidence channel deadline expired")


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise R07EvidenceError(f"{label} response is not JSON") from exc
    if not isinstance(decoded, dict):
        raise R07EvidenceError(f"{label} response is not a JSON object")
    return decoded


def _json_list(payload: dict[str, object], key: str, *, label: str) -> list[dict[str, object]]:
    values = payload.get(key)
    if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
        raise R07EvidenceError(f"{label} response has no {key} array")
    return [item for item in values if isinstance(item, dict)]


def _select_workflow_run(runs: list[dict[str, object]], commit_sha: str) -> int:
    identities = {
        run.get("id")
        for run in runs
        if run.get("path") == EVIDENCE_WORKFLOW_PATH
        and run.get("event") == "push"
        and run.get("head_branch") == "main"
        and run.get("head_sha") == commit_sha
        and isinstance(run.get("id"), int)
        and not isinstance(run.get("id"), bool)
    }
    if len(identities) != 1:
        raise R07EvidenceError(
            "the fixed workflow must report exactly one push main run identity "
            f"for the target commit, found {len(identities)}"
        )
    return int(next(iter(identities)))


def _select_run_attempt(runs: list[dict[str, object]], run_id: int) -> int:
    attempts = [
        run["run_attempt"]
        for run in runs
        if run.get("id") == run_id
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        and isinstance(run.get("run_attempt"), int)
        and not isinstance(run.get("run_attempt"), bool)
    ]
    if not attempts:
        raise R07EvidenceError("the push main run has no completed successful attempt")
    return max(int(attempt) for attempt in attempts)


def _select_artifact_url(
    artifacts: list[dict[str, object]],
    *,
    commit_sha: str,
) -> str:
    expected = f"r07-dr-gate-{commit_sha}"
    matches = [
        artifact
        for artifact in artifacts
        if artifact.get("name") == expected and artifact.get("expired") is False
    ]
    if len(matches) != 1:
        raise R07EvidenceError(
            f"the run must publish exactly one unexpired {expected} artifact, "
            f"found {len(matches)}"
        )
    url = matches[0].get("archive_download_url")
    if not isinstance(url, str) or not url:
        raise R07EvidenceError("the bound artifact has no archive download URL")
    return url


def _extract_artifact_json(archive: bytes) -> bytes:
    try:
        bundle = zipfile.ZipFile(io.BytesIO(archive))
    except (zipfile.BadZipFile, OSError, ValueError) as exc:
        raise R07EvidenceError("the downloaded artifact is not a readable zip archive") from exc
    with bundle:
        if bundle.namelist() != [EVIDENCE_ARTIFACT_JSON_PATH]:
            raise R07EvidenceError("the artifact must contain exactly the fixed evidence JSON path")
        info = bundle.getinfo(EVIDENCE_ARTIFACT_JSON_PATH)
        if info.file_size > _MAX_RESPONSE_BYTES:
            raise R07EvidenceError("the artifact evidence entry is implausibly large")
        try:
            return bundle.read(EVIDENCE_ARTIFACT_JSON_PATH)
        except (zipfile.BadZipFile, OSError, ValueError) as exc:
            raise R07EvidenceError("the artifact evidence entry is unreadable") from exc


def download_evidence_bytes(
    *,
    commit_sha: str,
    transport: EvidenceTransport,
    token_provider: TokenProvider,
    clock: Callable[[], float] = time.monotonic,
    budget_seconds: float = _DOWNLOAD_BUDGET_SECONDS,
) -> tuple[bytes, ResolvedRunIdentityV1]:
    """Fetch the fixed artifact for one exact target SHA from the fixed workflow channel.

    Returns the artifact bytes together with the run identity and the highest completed
    successful attempt that were resolved for them, so the caller can bind the accepted evidence
    to that exact pair.
    """

    token = token_provider.token()
    deadline = _DownloadDeadline(clock, budget_seconds)
    deadline.check()
    runs_payload = _json_object(
        transport.get(
            workflow_runs_url(commit_sha),
            token=token,
            accept="application/vnd.github+json",
        ),
        label="workflow runs",
    )
    runs = _json_list(runs_payload, "workflow_runs", label="workflow runs")
    run_id = _select_workflow_run(runs, commit_sha)
    identity = ResolvedRunIdentityV1(
        workflow_run_id=run_id,
        run_attempt=_select_run_attempt(runs, run_id),
    )
    deadline.check()
    artifacts_payload = _json_object(
        transport.get(
            run_artifacts_url(run_id),
            token=token,
            accept="application/vnd.github+json",
        ),
        label="run artifacts",
    )
    artifacts = _json_list(artifacts_payload, "artifacts", label="run artifacts")
    archive_url = _select_artifact_url(artifacts, commit_sha=commit_sha)
    deadline.check()
    archive = transport.get(archive_url, token=token, accept="application/vnd.github+json")
    raw = _extract_artifact_json(archive)
    try:
        wire = R07DrGateEvidenceWireV1.from_canonical_json(raw)
    except (ValidationError, ValueError) as exc:
        raise R07EvidenceError(
            f"the downloaded artifact is not strict canonical wire JSON: {exc}"
        ) from exc
    if wire.candidate_commit_sha != commit_sha:
        raise R07EvidenceError("the downloaded artifact is bound to another candidate commit")
    if wire.artifact_name != f"r07-dr-gate-{commit_sha}":
        raise R07EvidenceError("the downloaded artifact name is not bound to the target commit")
    if (wire.workflow_run_id, wire.run_attempt) != (identity.workflow_run_id, identity.run_attempt):
        raise R07EvidenceError(
            "the downloaded artifact does not name the resolved push main run identity and attempt"
        )
    return raw, identity


class R07DeployEvidenceGate:
    """Resolve the Release A/B decision and the enforced evidence before any mutation."""

    def __init__(
        self,
        *,
        cache_dir: Path,
        transport: EvidenceTransport | None = None,
        token_provider: TokenProvider | None = None,
        clock: Callable[[], float] = time.monotonic,
        verifier: EvidenceVerifier = DEFAULT_EVIDENCE_VERIFIER,
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._transport = transport or UrllibEvidenceTransport()
        self._token_provider = token_provider or EnvironmentTokenProvider()
        self._clock = clock
        self._verifier = verifier

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def evaluate(
        self,
        *,
        repo: Path,
        runner: CommandRunner,
        git_path: Path,
        installed_commit_sha: str,
        target_commit_sha: str,
    ) -> R07DeployDecision:
        try:
            installed_state, _installed_policy = read_policy_state(
                runner=runner,
                git_path=git_path,
                commit_sha=installed_commit_sha,
                role="installed",
            )
            target_state, target_policy = read_policy_state(
                runner=runner,
                git_path=git_path,
                commit_sha=target_commit_sha,
                role="target",
            )
        except (R07EvidenceError, ValidationError, ValueError, OSError) as exc:
            return _unreadable_decision(installed_commit_sha, target_commit_sha, exc)
        decision = decide_r07_deployment(
            cast("InstalledPolicyState", installed_state),
            cast("TargetPolicyState", target_state),
        )
        if not decision.requires_evidence:
            return decision
        if target_policy is None:  # pragma: no cover - enforced targets always parse a policy
            raise R07EvidenceError("an enforced target must resolve its policy Git object")
        try:
            self._require_verified_evidence(
                repo=Path(repo),
                policy=target_policy,
                commit_sha=target_state.commit_sha,
                tree_sha=target_state.tree_sha,
            )
        except (
            R07EvidenceError,
            ValidationError,
            ValueError,
            OSError,
            subprocess.CalledProcessError,
        ) as exc:
            return _decision(
                cast("InstalledPolicyState", installed_state),
                cast("TargetPolicyState", target_state),
                allowed=False,
                gate="rejected",
                reason=f"enforced evidence was refused: {exc}",
            )
        return decision

    def _require_verified_evidence(
        self,
        *,
        repo: Path,
        policy: R07PolicyV1,
        commit_sha: str,
        tree_sha: str,
    ) -> R07DrGateEvidenceWireV1:
        cached = read_cached_evidence(
            cache_dir=self._cache_dir,
            commit_sha=commit_sha,
            tree_sha=tree_sha,
            policy=policy,
        )
        if cached is None:
            raw, identity = download_evidence_bytes(
                commit_sha=commit_sha,
                transport=self._transport,
                token_provider=self._token_provider,
                clock=self._clock,
            )
            downloaded = bind_evidence_wire(
                raw,
                commit_sha=commit_sha,
                tree_sha=tree_sha,
                policy=policy,
                run_identity=identity,
            )
            self._verify(repo, policy, downloaded)
            write_cached_evidence(
                cache_dir=self._cache_dir,
                commit_sha=commit_sha,
                payload=raw,
            )
            cached = read_cached_evidence(
                cache_dir=self._cache_dir,
                commit_sha=commit_sha,
                tree_sha=tree_sha,
                policy=policy,
            )
            if cached is None:
                raise R07EvidenceError("the retained evidence cache entry disappeared")
        self._verify(repo, policy, cached)
        return cached

    def _verify(
        self,
        repo: Path,
        policy: R07PolicyV1,
        wire: R07DrGateEvidenceWireV1,
    ) -> None:
        try:
            self._verifier(repo, policy, wire)
        except (ValidationError, ValueError, OSError, subprocess.CalledProcessError) as exc:
            raise R07EvidenceError(f"the private verifier refused the evidence: {exc}") from exc


def _reportable_sha(value: str) -> str:
    if len(value) == 40 and all(char in "0123456789abcdef" for char in value):
        return value
    return "0" * 40


def _unreadable_decision(
    installed_commit_sha: str,
    target_commit_sha: str,
    error: Exception,
) -> R07DeployDecision:
    return R07DeployDecision(
        allowed=False,
        gate="rejected",
        reason=f"R07 policy objects could not be resolved: {error}",
        requires_evidence=False,
        installed_mode="unresolved",
        target_mode="unresolved",
        installed_commit_sha=_reportable_sha(installed_commit_sha),
        installed_tree_sha="0" * 40,
        target_commit_sha=_reportable_sha(target_commit_sha),
        target_tree_sha="0" * 40,
    )
