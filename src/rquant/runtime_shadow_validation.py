"""Immutable parity evidence and retirement gates for legacy runtime shadows."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import shutil
import stat
import subprocess
import tempfile
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, time
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StrictInt, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_canonical_json_loads,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MICROSECONDS_PER_SECOND = 1_000_000
_MAX_MATCH_TOLERANCE_MICROSECONDS = 30 * 60 * _MICROSECONDS_PER_SECOND
_MAX_REPORT_BYTES = 128 * 1024 * 1024
_MAX_REPORT_OBSERVATIONS = 100_000
_MAX_REPORT_INPUT_BYTES = 128 * 1024 * 1024
_MAX_REPORT_JSON_DEPTH = 64
_MAX_REPORT_JSON_NODES = 1_000_000
ShadowPublicationFaultHook = Callable[[str], None]


def _preflight_report_json(payload: bytes) -> None:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("shadow report is not valid UTF-8") from exc
    depth = 0
    nodes = 0
    in_string = False
    escaped = False
    in_atom = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if in_atom:
            if character not in " \t\r\n,]}":
                continue
            in_atom = False
        if character == '"':
            in_string = True
            nodes += 1
        elif character in "[{":
            depth += 1
            nodes += 1
            if depth > _MAX_REPORT_JSON_DEPTH:
                raise ValueError("shadow report exceeds the JSON depth budget")
        elif character in "]}":
            depth -= 1
        elif character not in " \t\r\n,:":
            in_atom = True
            nodes += 1
        if nodes > _MAX_REPORT_JSON_NODES:
            raise ValueError("shadow report exceeds the JSON node budget")


def _closed_authoritative_dates(
    authority: MarketCalendarAuthority,
    evaluated_at,
) -> tuple[date, ...]:
    observed = normalize_aware_utc(evaluated_at)
    if authority.generated_at > observed:
        raise ValueError("calendar authority was generated after evaluation")
    if any(item.weekday() >= 5 for item in authority.open_dates):
        raise ValueError("authoritative open dates cannot include a weekend")
    local = observed.astimezone(_SHANGHAI)
    if local.date() < authority.coverage_start or local.date() > authority.coverage_end:
        raise ValueError("calendar evaluation is outside authority coverage")
    closed_through = local.date()
    if local.timetz().replace(tzinfo=None) < time(15, 0):
        closed_through = local.date().fromordinal(local.date().toordinal() - 1)
    closed = tuple(item for item in authority.open_dates if item <= closed_through)
    if not closed:
        raise ValueError("calendar has no closed trading session at evaluation time")
    return closed


class ShadowStrategyBinding(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256


class ShadowObservation(RuntimeContractModel):
    observation_id: Sha256 | None = None
    source: Literal["legacy", "isolated"]
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    trade_date: date
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    action: str = Field(min_length=1)
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    availability_basis: Literal["observed_completion", "export_observed_proxy"] = (
        "observed_completion"
    )
    producer_commit: CommitSha
    upstream_event_id: Sha256
    evidence_id: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.event_time.date() != self.trade_date:
            raise ValueError("shadow observation event date mismatch")
        if self.available_at < self.event_time:
            raise ValueError("shadow observation cannot be available before its event")
        if self.source == "isolated" and self.availability_basis != "observed_completion":
            raise ValueError("isolated shadow availability must use observed completion")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"observation_id"}))
        if self.observation_id is None:
            object.__setattr__(self, "observation_id", expected)
        elif self.observation_id != expected:
            raise ValueError("shadow observation id does not match content")
        return self


class ShadowObservationMatch(RuntimeContractModel):
    legacy_observation_id: Sha256
    isolated_observation_id: Sha256
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    ts_code: str
    action: str
    event_delta_microseconds: StrictInt = Field(ge=0)
    availability_delta_microseconds: StrictInt | None = Field(default=None, ge=0)


class ShadowObservationDiscrepancy(RuntimeContractModel):
    legacy_observation_id: Sha256 | None = None
    isolated_observation_id: Sha256 | None = None
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    ts_code: str
    reason: Literal[
        "action_mismatch",
        "outside_time_tolerance",
        "legacy_only",
        "isolated_only",
    ]


class CompletionAttestationClaims(RuntimeContractModel):
    contract: Literal["runtime-completion-attestation/v2"] = "runtime-completion-attestation/v2"
    completion_receipt_body_contract: Literal["runtime-shadow-source-completion-body/v2"] = (
        "runtime-shadow-source-completion-body/v2"
    )
    completion_receipt_body_sha256: Sha256
    trade_date: date
    session_close_at: AwareUtcDatetime
    source_id: str = Field(min_length=1)
    input_identity: Sha256
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    strategy_registration_fingerprint: Sha256
    strategy_spec_fingerprint: Sha256
    executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    feature_registration_fingerprint: Sha256
    feature_contract_fingerprint: Sha256
    routing_policy_fingerprint: Sha256
    producer_manifest_fingerprint: Sha256
    producer_commit: CommitSha
    producer_version: str = Field(min_length=1)
    producer_service_id: str = Field(min_length=1)
    producer_instance_id: str = Field(min_length=1)
    calendar_generation_id: Sha256
    feature_source_generation_id: Sha256
    feature_close_marker_id: Sha256
    feature_segment_chain_hash: Sha256
    runner_generation_id: Sha256
    runner_segment_start_sequence: StrictInt = Field(ge=0)
    runner_segment_final_sequence: StrictInt = Field(ge=0)
    runner_segment_record_count: StrictInt = Field(ge=0)
    runner_segment_chain_hash: Sha256
    signal_authority_generation_id: Sha256
    route_receipts_id: Sha256

    @model_validator(mode="after")
    def validate_segment(self) -> CompletionAttestationClaims:
        if self.runner_segment_final_sequence < self.runner_segment_start_sequence:
            raise ValueError("completion attestation runner segment range is invalid")
        if self.runner_segment_record_count != (
            self.runner_segment_final_sequence - self.runner_segment_start_sequence
        ):
            raise ValueError("completion attestation runner segment count is invalid")
        return self


class CompletionAttestation(RuntimeContractModel):
    attestation_id: Sha256 | None = None
    key_id: str = Field(min_length=1)
    claims: CompletionAttestationClaims
    signature_algorithm: Literal["test-hmac-sha256", "ed25519"] = "test-hmac-sha256"
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_identity(self) -> CompletionAttestation:
        expected = canonical_sha256(
            {
                "contract": "runtime-completion-attestation-envelope/v1",
                "key_id": self.key_id,
                "claims": self.claims,
                "signature_algorithm": self.signature_algorithm,
                "signature": self.signature,
            }
        )
        if self.attestation_id is None:
            object.__setattr__(self, "attestation_id", expected)
        elif self.attestation_id != expected:
            raise ValueError("completion attestation identity mismatch")
        return self


class CompletionAttestationSigner(Protocol):
    def issue(self, claims: CompletionAttestationClaims) -> CompletionAttestation: ...


class CompletionAttestationVerifier(Protocol):
    def verify(self, attestation: CompletionAttestation) -> bool: ...


class CompletionAttestationSigningClient(Protocol):
    """External signing capability. Shadow domain code never receives its private key."""

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


_COMPLETION_ATTESTATION_NAMESPACE = "rquant-shadow-completion-attestation"
_REPORT_RECEIPT_NAMESPACE = "rquant-shadow-report-receipt"
_LEGACY_SHADOW_RECOVERY_NAMESPACE = "rquant-legacy-shadow-recovery-marker"
_ED25519_SIGNATURE_BYTES = 64


def _ed25519_signing_payload(*, namespace: str, payload: bytes) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "rquant-ed25519-domain-separation/v1",
            "namespace": namespace,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required for Ed25519 completion attestation verification")


def _verify_ed25519_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
    try:
        decoded = base64.b64decode(signature, validate=True)
    except (TypeError, ValueError):
        return False
    if len(decoded) != _ED25519_SIGNATURE_BYTES:
        return False
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-shadow-verify-") as directory_name:
            root = Path(directory_name)
            root.chmod(0o700)
            public_path = root / "public.pem"
            payload_path = root / "payload.bin"
            signature_path = root / "signature.bin"
            public_path.write_bytes(public_key)
            payload_path.write_bytes(payload)
            signature_path.write_bytes(decoded)
            for path in (public_path, payload_path, signature_path):
                path.chmod(0o600)
            completed = subprocess.run(
                (
                    _openssl_binary(),
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public_path),
                    "-sigfile",
                    str(signature_path),
                    "-rawin",
                    "-in",
                    str(payload_path),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5.0,
            )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return False
    return completed.returncode == 0


def _validate_ed25519_public_key(public_key: bytes) -> None:
    try:
        completed = subprocess.run(
            (
                _openssl_binary(),
                "pkey",
                "-pubin",
                "-pubcheck",
                "-text_pub",
                "-noout",
            ),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("completion attestation public key is not a usable Ed25519 key") from exc
    if completed.returncode != 0 or b"ED25519" not in completed.stdout.upper():
        raise ValueError("completion attestation public key is not an Ed25519 key")


class Ed25519CompletionAttestationKeyring:
    """Public-only active plus previous-key verifier for production completion receipts."""

    def __init__(
        self,
        *,
        active_key_id: str,
        active_public_key: bytes,
        previous_public_keys: Mapping[str, bytes] | None = None,
    ) -> None:
        if not active_key_id.strip() or any(character.isspace() for character in active_key_id):
            raise ValueError("completion attestation active key id is invalid")
        if not isinstance(active_public_key, bytes) or not active_public_key:
            raise ValueError("completion attestation active public key is invalid")
        previous = dict(previous_public_keys or {})
        if active_key_id in previous:
            raise ValueError("completion attestation active key cannot also be previous")
        if any(
            not key_id.strip()
            or any(character.isspace() for character in key_id)
            or not isinstance(public_key, bytes)
            or not public_key
            for key_id, public_key in previous.items()
        ):
            raise ValueError("completion attestation previous keyring is invalid")
        _validate_ed25519_public_key(active_public_key)
        for public_key in previous.values():
            _validate_ed25519_public_key(public_key)
        self.active_key_id = active_key_id
        self._keys = {active_key_id: active_public_key, **previous}

    @property
    def trusted_key_ids(self) -> tuple[str, ...]:
        return tuple(self._keys)

    @staticmethod
    def _payload(claims: CompletionAttestationClaims) -> bytes:
        return canonical_json_bytes(claims.model_dump(mode="json"))

    def verify(self, attestation: CompletionAttestation) -> bool:
        try:
            verified = CompletionAttestation.model_validate(attestation)
        except (TypeError, ValueError):
            return False
        if (
            verified.signature_algorithm != "ed25519"
            or verified.claims.contract != "runtime-completion-attestation/v2"
        ):
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None:
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_COMPLETION_ATTESTATION_NAMESPACE,
                payload=self._payload(verified.claims),
            ),
            signature=verified.signature,
        )


class Ed25519CompletionAttestationSigner:
    """A signer client adapter; private key material stays behind the client boundary."""

    def __init__(self, *, key_id: str, client: CompletionAttestationSigningClient) -> None:
        if not key_id.strip() or any(character.isspace() for character in key_id):
            raise ValueError("completion attestation signing key id is invalid")
        self.key_id = key_id
        self._client = client

    @staticmethod
    def _payload(claims: CompletionAttestationClaims) -> bytes:
        return canonical_json_bytes(claims.model_dump(mode="json"))

    def issue(self, claims: CompletionAttestationClaims) -> CompletionAttestation:
        verified = CompletionAttestationClaims.model_validate(claims)
        signature = self._client.sign(
            namespace=_COMPLETION_ATTESTATION_NAMESPACE,
            payload=_ed25519_signing_payload(
                namespace=_COMPLETION_ATTESTATION_NAMESPACE,
                payload=self._payload(verified),
            ),
        )
        if not _valid_ed25519_signature(signature):
            raise ValueError("completion attestation signing client returned an invalid signature")
        return CompletionAttestation(
            key_id=self.key_id,
            claims=verified,
            signature_algorithm="ed25519",
            signature=signature,
        )


def _valid_ed25519_signature(signature: str) -> bool:
    try:
        return len(base64.b64decode(signature, validate=True)) == _ED25519_SIGNATURE_BYTES
    except (TypeError, ValueError):
        return False


class ShadowSigningRequest(RuntimeContractModel):
    """Bounded stdin protocol accepted by the root-owned Shadow signer."""

    schema_version: Literal[1, 2] = 1
    operation: Literal[
        "sign",
        "capture-recovery",
        "sign-recovery",
        "resume-recovery",
    ] = "sign"
    request_id: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_base64: str = Field(min_length=1, max_length=2 * 1024 * 1024)
    payload_sha256: Sha256

    @model_validator(mode="after")
    def validate_protocol_version(self) -> Self:
        expected = 1 if self.operation == "sign" else 2
        if self.schema_version != expected:
            raise ValueError("Shadow signing request protocol version mismatch")
        return self


class ShadowSigningResponse(RuntimeContractModel):
    """Canonical response from the root-owned Shadow signer."""

    schema_version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    request_id: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_sha256: Sha256
    signature: str = Field(min_length=1, max_length=16_384)


class ShadowProtectedSigningResponse(RuntimeContractModel):
    """Authoritative payload returned by a structured root signing operation."""

    schema_version: Literal[2] = 2
    operation: Literal["capture-recovery", "sign-recovery", "resume-recovery"]
    request_id: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    namespace: Literal["rquant-legacy-shadow-recovery-marker"]
    request_payload_sha256: Sha256
    signed_payload_base64: str = Field(min_length=1, max_length=2 * 1024 * 1024)
    signed_payload_sha256: Sha256
    signature: str = Field(min_length=1, max_length=16_384)


class SecureShadowSigningClient:
    """One protected signing capability; Shadow never receives private-key paths."""

    def __init__(
        self,
        *,
        command: tuple[str, ...],
        key_id: str,
        timeout_seconds: float,
    ) -> None:
        if not command or any(not item for item in command):
            raise ValueError("Shadow signer capability command is invalid")
        if not key_id.strip() or any(character.isspace() for character in key_id):
            raise ValueError("Shadow signer key id is invalid")
        self._command = tuple(command)
        self.key_id = key_id
        self.timeout_seconds = timeout_seconds

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in {
            _COMPLETION_ATTESTATION_NAMESPACE,
            _REPORT_RECEIPT_NAMESPACE,
        }:
            raise ValueError("Shadow signer namespace is not allowed")
        if not payload or len(payload) > _MAX_REPORT_BYTES:
            raise ValueError("Shadow signer payload is invalid")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        request_id = canonical_sha256(
            {
                "contract": "runtime-shadow-signing-request/v1",
                "key_id": self.key_id,
                "namespace": namespace,
                "payload_sha256": payload_sha256,
            }
        )
        request = ShadowSigningRequest(
            request_id=request_id,
            key_id=self.key_id,
            namespace=namespace,
            payload_base64=base64.b64encode(payload).decode("ascii"),
            payload_sha256=payload_sha256,
        )
        try:
            completed = subprocess.run(
                self._command,
                input=canonical_json_bytes(request.model_dump(mode="json")),
                check=False,
                capture_output=True,
                timeout=self.timeout_seconds,
                env={"LANG": "C", "PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("Shadow signer capability is unavailable") from exc
        if completed.returncode != 0 or not completed.stdout:
            raise RuntimeError("Shadow signer capability failed")
        try:
            response = ShadowSigningResponse.model_validate(
                strict_canonical_json_loads(completed.stdout)
            )
        except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("Shadow signer capability returned an invalid response") from exc
        if (
            response.request_id != request.request_id
            or response.key_id != request.key_id
            or response.namespace != request.namespace
            or response.payload_sha256 != request.payload_sha256
            or not _valid_ed25519_signature(response.signature)
        ):
            raise RuntimeError("Shadow signer capability response does not match")
        return response.signature

    def capture_legacy_recovery(self, *, payload: bytes) -> tuple[bytes, str]:
        return self._structured_recovery_operation(
            operation="capture-recovery",
            payload=payload,
        )

    def sign_legacy_recovery(self, *, payload: bytes) -> tuple[bytes, str]:
        return self._structured_recovery_operation(
            operation="sign-recovery",
            payload=payload,
        )

    def resume_legacy_recovery(self, *, payload: bytes) -> tuple[bytes, str]:
        return self._structured_recovery_operation(
            operation="resume-recovery",
            payload=payload,
        )

    def _structured_recovery_operation(
        self,
        *,
        operation: Literal["capture-recovery", "sign-recovery", "resume-recovery"],
        payload: bytes,
    ) -> tuple[bytes, str]:
        if not payload or len(payload) > _MAX_REPORT_BYTES:
            raise ValueError("Shadow recovery signer payload is invalid")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        request_id = canonical_sha256(
            {
                "contract": "runtime-shadow-signing-request/v2",
                "operation": operation,
                "key_id": self.key_id,
                "namespace": _LEGACY_SHADOW_RECOVERY_NAMESPACE,
                "payload_sha256": payload_sha256,
            }
        )
        request = ShadowSigningRequest(
            schema_version=2,
            operation=operation,
            request_id=request_id,
            key_id=self.key_id,
            namespace=_LEGACY_SHADOW_RECOVERY_NAMESPACE,
            payload_base64=base64.b64encode(payload).decode("ascii"),
            payload_sha256=payload_sha256,
        )
        try:
            completed = subprocess.run(
                self._command,
                input=canonical_json_bytes(request.model_dump(mode="json")),
                check=False,
                capture_output=True,
                timeout=self.timeout_seconds,
                env={"LANG": "C", "PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("Shadow recovery signer capability is unavailable") from exc
        if completed.returncode != 0 or not completed.stdout:
            detail = completed.stderr.decode("utf-8", errors="replace").strip()[:512]
            suffix = f": {detail}" if detail else ""
            raise RuntimeError(f"Shadow recovery signer capability failed{suffix}")
        try:
            response = ShadowProtectedSigningResponse.model_validate(
                strict_canonical_json_loads(completed.stdout)
            )
            signed_payload = base64.b64decode(
                response.signed_payload_base64,
                validate=True,
            )
        except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError(
                "Shadow recovery signer capability returned an invalid response"
            ) from exc
        if (
            response.operation != operation
            or response.request_id != request.request_id
            or response.key_id != request.key_id
            or response.namespace != request.namespace
            or response.request_payload_sha256 != request.payload_sha256
            or hashlib.sha256(signed_payload).hexdigest() != response.signed_payload_sha256
            or not _valid_ed25519_signature(response.signature)
        ):
            raise RuntimeError("Shadow recovery signer capability response does not match")
        return signed_payload, response.signature


class HmacCompletionAttestationAuthority:
    """Test-only compatibility authority. Production uses Ed25519 keyrings."""

    def __init__(self, *, key_id: str, secret: bytes) -> None:
        if not key_id.strip():
            raise ValueError("completion attestation key_id cannot be empty")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("completion attestation secret must contain at least 32 bytes")
        self.key_id = key_id
        self._secret = secret

    @staticmethod
    def _payload(claims: CompletionAttestationClaims) -> bytes:
        return canonical_json_bytes(claims.model_dump(mode="json"))

    def issue(self, claims: CompletionAttestationClaims) -> CompletionAttestation:
        verified = CompletionAttestationClaims.model_validate(claims)
        signature = hmac.new(self._secret, self._payload(verified), hashlib.sha256).hexdigest()
        return CompletionAttestation(
            key_id=self.key_id,
            claims=verified,
            signature_algorithm="test-hmac-sha256",
            signature=signature,
        )

    def verify(self, attestation: CompletionAttestation) -> bool:
        verified = CompletionAttestation.model_validate(attestation)
        if (
            verified.key_id != self.key_id
            or verified.signature_algorithm != "test-hmac-sha256"
            or verified.claims.contract != "runtime-completion-attestation/v2"
        ):
            return False
        expected = hmac.new(
            self._secret,
            self._payload(verified.claims),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, verified.signature)


class ShadowSourceCompletionReceipt(RuntimeContractModel):
    receipt_id: Sha256 | None = None
    evidence_origin: Literal["production", "test_fixture"]
    source: Literal["legacy", "isolated"]
    source_id: str = Field(min_length=1)
    trade_date: date
    session_close_at: AwareUtcDatetime
    complete_through: AwareUtcDatetime
    input_identity: Sha256
    produced_at: AwareUtcDatetime
    producer_commit: CommitSha
    producer_version: str = Field(min_length=1)
    producer_service_id: str | None = Field(default=None, min_length=1)
    producer_instance_id: str | None = Field(default=None, min_length=1)
    runner_generation_id: Sha256 | None = None
    signal_authority_generation_id: Sha256 | None = None
    calendar_generation_id: Sha256 | None = None
    last_sequence: StrictInt | None = Field(default=None, ge=-1)
    high_watermark: StrictInt | None = Field(default=None, ge=0)
    route_receipts_id: Sha256 | None = None
    feature_source_generation_id: Sha256 | None = None
    feature_close_marker_id: Sha256 | None = None
    feature_segment_chain_hash: Sha256 | None = None
    segment_start_sequence: StrictInt | None = Field(default=None, ge=0)
    segment_record_count: StrictInt | None = Field(default=None, ge=0)
    segment_chain_hash: Sha256 | None = None
    completion_attestation: CompletionAttestation | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        local_close = self.session_close_at.astimezone(_SHANGHAI)
        if local_close.date() != self.trade_date:
            raise ValueError("completion receipt trade date does not match session close")
        if local_close.timetz().replace(tzinfo=None) != time(15, 0):
            raise ValueError("completion receipt must bind the 15:00 session close")
        if self.complete_through > self.produced_at:
            raise ValueError("completion receipt cannot cover data after it was produced")
        isolated_authority = (
            self.producer_service_id,
            self.producer_instance_id,
            self.runner_generation_id,
            self.signal_authority_generation_id,
            self.calendar_generation_id,
            self.last_sequence,
            self.high_watermark,
            self.route_receipts_id,
            self.feature_source_generation_id,
            self.feature_close_marker_id,
            self.feature_segment_chain_hash,
            self.segment_start_sequence,
            self.segment_record_count,
            self.segment_chain_hash,
        )
        if self.evidence_origin == "production" and self.source == "isolated":
            if any(value is None for value in isolated_authority):
                raise ValueError(
                    "production isolated completion requires durable authority identities"
                )
        elif any(value is not None for value in isolated_authority):
            raise ValueError(
                "runner completion authority is only valid for production isolated evidence"
            )
        if self.completion_attestation is not None:
            if self.evidence_origin != "production" or self.source != "isolated":
                raise ValueError(
                    "completion attestation is only valid for production isolated evidence"
                )
            claims = self.completion_attestation.claims
            expected_claims = {
                "trade_date": self.trade_date,
                "session_close_at": self.session_close_at,
                "source_id": self.source_id,
                "input_identity": self.input_identity,
                "producer_commit": self.producer_commit,
                "producer_version": self.producer_version,
                "producer_service_id": self.producer_service_id,
                "producer_instance_id": self.producer_instance_id,
                "calendar_generation_id": self.calendar_generation_id,
                "feature_source_generation_id": self.feature_source_generation_id,
                "feature_close_marker_id": self.feature_close_marker_id,
                "feature_segment_chain_hash": self.feature_segment_chain_hash,
                "runner_generation_id": self.runner_generation_id,
                "runner_segment_start_sequence": self.segment_start_sequence,
                "runner_segment_final_sequence": self.high_watermark,
                "runner_segment_record_count": self.segment_record_count,
                "runner_segment_chain_hash": self.segment_chain_hash,
                "signal_authority_generation_id": self.signal_authority_generation_id,
                "route_receipts_id": self.route_receipts_id,
            }
            for name, expected_value in expected_claims.items():
                if getattr(claims, name) != expected_value:
                    raise ValueError(f"completion attestation claim does not match receipt: {name}")
            if claims.completion_receipt_body_sha256 != shadow_completion_receipt_body_sha256(self):
                raise ValueError("completion attestation does not bind the full receipt body")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("completion receipt id does not match content")
        return self


def shadow_completion_receipt_body_sha256(receipt: ShadowSourceCompletionReceipt) -> str:
    """Hash every completion semantic except its envelope identity and attestation."""

    if not isinstance(receipt, ShadowSourceCompletionReceipt):
        raise TypeError("completion receipt body hash requires a validated receipt")
    return canonical_sha256(
        {
            "contract": "runtime-shadow-source-completion-body/v2",
            "body": receipt.model_dump(
                mode="python",
                exclude={"receipt_id", "completion_attestation"},
            ),
        }
    )


def verify_completion_attestation(
    receipt: ShadowSourceCompletionReceipt,
    verifier: CompletionAttestationVerifier | None,
) -> bool:
    attestation = receipt.completion_attestation
    if attestation is None or verifier is None:
        return False
    try:
        if (
            attestation.claims.contract != "runtime-completion-attestation/v2"
            or attestation.claims.completion_receipt_body_sha256
            != shadow_completion_receipt_body_sha256(receipt)
        ):
            return False
        return bool(verifier.verify(attestation))
    except (TypeError, ValueError, RecursionError):
        return False


def shadow_upstream_snapshot_id(
    raw_input_id: str,
    receipt: ShadowSourceCompletionReceipt,
) -> str:
    return canonical_sha256(
        {
            "contract": "runtime-shadow-upstream-snapshot/v2",
            "raw_input_id": raw_input_id,
            "completion_receipt": receipt,
        }
    )


class ShadowInputSnapshotIdentity(RuntimeContractModel):
    snapshot_id: Sha256 | None = None
    source: Literal["legacy", "isolated"]
    source_id: str = Field(min_length=1)
    binding: ShadowStrategyBinding
    raw_input_id: Sha256
    completion_receipt: ShadowSourceCompletionReceipt
    upstream_snapshot_id: Sha256
    observation_set_id: Sha256
    captured_at: AwareUtcDatetime
    complete_through: AwareUtcDatetime
    producer_commit: CommitSha
    producer_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.complete_through > self.captured_at:
            raise ValueError("shadow input cannot be complete after it was captured")
        receipt = self.completion_receipt
        if receipt.source != self.source or receipt.source_id != self.source_id:
            raise ValueError("completion receipt does not match shadow input source")
        if receipt.input_identity != self.raw_input_id:
            raise ValueError("completion receipt does not bind the raw shadow input")
        if receipt.complete_through != self.complete_through:
            raise ValueError("shadow completeness must come from its producer receipt")
        if receipt.produced_at > self.captured_at:
            raise ValueError("completion receipt was unavailable when shadow input was captured")
        if (
            receipt.producer_commit != self.producer_commit
            or receipt.producer_version != self.producer_version
        ):
            raise ValueError("completion receipt producer does not match shadow input producer")
        if self.upstream_snapshot_id != shadow_upstream_snapshot_id(
            self.raw_input_id,
            receipt,
        ):
            raise ValueError("shadow upstream snapshot does not bind raw input and receipt")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"snapshot_id"}))
        if self.snapshot_id is None:
            object.__setattr__(self, "snapshot_id", expected)
        elif self.snapshot_id != expected:
            raise ValueError("shadow input snapshot id does not match content")
        return self


class ShadowSessionEvidence(RuntimeContractModel):
    evidence_id: Sha256 | None = None
    evidence_origin: Literal["production", "test_fixture"]
    calendar_authority_id: Sha256
    evaluation_cutoff: AwareUtcDatetime
    session_open_at: AwareUtcDatetime
    session_close_at: AwareUtcDatetime
    producer_commit: CommitSha
    producer_version: str = Field(min_length=1)
    input_snapshots: tuple[ShadowInputSnapshotIdentity, ...]

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        local_open = self.session_open_at.astimezone(_SHANGHAI)
        local_close = self.session_close_at.astimezone(_SHANGHAI)
        if local_open.date() != local_close.date():
            raise ValueError("shadow evidence session boundaries must share a trade date")
        if local_open.timetz().replace(tzinfo=None) != time(9, 25):
            raise ValueError("shadow evidence session must start at 09:25 Asia/Shanghai")
        if local_close.timetz().replace(tzinfo=None) != time(15, 0):
            raise ValueError("shadow evidence session must close at 15:00 Asia/Shanghai")
        if self.session_open_at >= self.session_close_at:
            raise ValueError("shadow evidence session boundaries are invalid")
        if self.evaluation_cutoff < self.session_close_at:
            raise ValueError("shadow evidence cutoff is before session close")
        snapshot_keys = [
            (
                item.source,
                item.binding.strategy_id,
                item.binding.strategy_version,
                item.binding.definition_fingerprint,
                item.binding.executable_fingerprint,
            )
            for item in self.input_snapshots
        ]
        if len(snapshot_keys) != len(set(snapshot_keys)):
            raise ValueError("shadow input snapshots must have unique source bindings")
        for snapshot in self.input_snapshots:
            receipt = snapshot.completion_receipt
            if self.evidence_origin == "production" and receipt.evidence_origin != "production":
                raise ValueError(
                    "production shadow evidence requires production completion receipts"
                )
            if (
                self.evidence_origin == "production"
                and snapshot.source == "isolated"
                and receipt.calendar_generation_id != self.calendar_authority_id
            ):
                raise ValueError(
                    "isolated completion receipt calendar does not match "
                    "the shadow evidence calendar authority"
                )
            if receipt.trade_date != local_close.date():
                raise ValueError("completion receipt does not match the evidence trade date")
            if receipt.session_close_at != self.session_close_at:
                raise ValueError("completion receipt does not bind the evidence session close")
            if snapshot.captured_at > self.evaluation_cutoff:
                raise ValueError("shadow input was captured after the evidence cutoff")
            if receipt.produced_at > self.evaluation_cutoff:
                raise ValueError("completion receipt was unavailable at the evidence cutoff")
            if snapshot.complete_through < self.session_close_at:
                raise ValueError("shadow input does not cover the complete session")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"evidence_id"}))
        if self.evidence_id is None:
            object.__setattr__(self, "evidence_id", expected)
        elif self.evidence_id != expected:
            raise ValueError("shadow session evidence id does not match content")
        return self


class _DerivedReport(RuntimeContractModel):
    legacy_observation_set_id: Sha256
    isolated_observation_set_id: Sha256
    legacy_count: int = Field(ge=0)
    isolated_count: int = Field(ge=0)
    matched_count: int = Field(ge=0)
    legacy_only_count: int = Field(ge=0)
    isolated_only_count: int = Field(ge=0)
    legacy_recall_bps: int = Field(ge=0, le=10_000)
    isolated_precision_bps: int = Field(ge=0, le=10_000)
    comparable_latency_match_count: int = Field(ge=0)
    p95_latency_delta_microseconds: StrictInt | None = Field(default=None, ge=0)
    isolated_latency_measurement_count: int = Field(ge=0)
    isolated_latency_coverage_bps: int = Field(ge=0, le=10_000)
    isolated_p95_latency_microseconds: StrictInt = Field(ge=0)
    matches: tuple[ShadowObservationMatch, ...]
    discrepancies: tuple[ShadowObservationDiscrepancy, ...]


class ShadowReportReceiptClaims(RuntimeContractModel):
    """Signed completion record for one immutable daily shadow report."""

    contract: Literal["runtime-shadow-report-receipt/v1"] = "runtime-shadow-report-receipt/v1"
    trade_date: date
    strategy_bindings: tuple[ShadowStrategyBinding, ...] = Field(min_length=1)
    producer_service_id: str = Field(min_length=1)
    producer_instance_id: str = Field(min_length=1)
    code_commit: CommitSha
    producer_commit: CommitSha
    producer_version: str = Field(min_length=1)
    calendar_generation_id: Sha256
    source_generation_id: Sha256
    input_hash: Sha256
    output_hash: Sha256

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        canonical = tuple(
            sorted(
                self.strategy_bindings,
                key=lambda item: (
                    item.strategy_id,
                    item.strategy_version,
                    item.definition_fingerprint,
                    item.executable_fingerprint,
                ),
            )
        )
        if len(canonical) != len(set(canonical)):
            raise ValueError("shadow report receipt strategy bindings must be unique")
        object.__setattr__(self, "strategy_bindings", canonical)
        return self


class ShadowReportReceipt(RuntimeContractModel):
    receipt_id: Sha256 | None = None
    key_id: str = Field(min_length=1)
    claims: ShadowReportReceiptClaims
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if not _valid_ed25519_signature(self.signature):
            raise ValueError("shadow report receipt signature is not an Ed25519 signature")
        expected = canonical_sha256(
            {
                "contract": "runtime-shadow-report-receipt-envelope/v1",
                "key_id": self.key_id,
                "claims": self.claims,
                "signature": self.signature,
            }
        )
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("shadow report receipt identity mismatch")
        return self


class ShadowReportReceiptSigner(Protocol):
    def issue_report(self, claims: ShadowReportReceiptClaims) -> ShadowReportReceipt: ...


class ShadowReportReceiptVerifier(Protocol):
    def verify_report(self, receipt: ShadowReportReceipt) -> bool: ...


class Ed25519ShadowReceiptSigner(Ed25519CompletionAttestationSigner):
    """One opaque signer client can attest both runner completion and report publication."""

    def issue_report(self, claims: ShadowReportReceiptClaims) -> ShadowReportReceipt:
        verified = ShadowReportReceiptClaims.model_validate(claims)
        signature = self._client.sign(
            namespace=_REPORT_RECEIPT_NAMESPACE,
            payload=_ed25519_signing_payload(
                namespace=_REPORT_RECEIPT_NAMESPACE,
                payload=canonical_json_bytes(verified.model_dump(mode="json")),
            ),
        )
        if not _valid_ed25519_signature(signature):
            raise ValueError("shadow report signing client returned an invalid signature")
        return ShadowReportReceipt(
            key_id=self.key_id,
            claims=verified,
            signature=signature,
        )


class Ed25519ShadowReceiptKeyring(Ed25519CompletionAttestationKeyring):
    """Public-only verifier with an explicit active plus previous trust set."""

    def verify_report(self, receipt: ShadowReportReceipt) -> bool:
        try:
            verified = ShadowReportReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None:
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_REPORT_RECEIPT_NAMESPACE,
                payload=canonical_json_bytes(verified.claims.model_dump(mode="json")),
            ),
            signature=verified.signature,
        )


class ShadowSessionReport(RuntimeContractModel):
    report_id: Sha256 | None = None
    evidence_origin: Literal["production", "test_fixture"] = "test_fixture"
    evidence: ShadowSessionEvidence | None = None
    trade_date: date
    match_tolerance_microseconds: StrictInt = Field(
        ge=0,
        le=_MAX_MATCH_TOLERANCE_MICROSECONDS,
    )
    legacy_observations: tuple[ShadowObservation, ...]
    isolated_observations: tuple[ShadowObservation, ...]
    legacy_observation_set_id: Sha256
    isolated_observation_set_id: Sha256
    legacy_count: int = Field(ge=0)
    isolated_count: int = Field(ge=0)
    matched_count: int = Field(ge=0)
    legacy_only_count: int = Field(ge=0)
    isolated_only_count: int = Field(ge=0)
    legacy_recall_bps: int = Field(ge=0, le=10_000)
    isolated_precision_bps: int = Field(ge=0, le=10_000)
    comparable_latency_match_count: int = Field(ge=0)
    p95_latency_delta_microseconds: StrictInt | None = Field(default=None, ge=0)
    isolated_latency_measurement_count: int = Field(ge=0)
    isolated_latency_coverage_bps: int = Field(ge=0, le=10_000)
    isolated_p95_latency_microseconds: StrictInt = Field(ge=0)
    matches: tuple[ShadowObservationMatch, ...]
    discrepancies: tuple[ShadowObservationDiscrepancy, ...]
    report_receipt: ShadowReportReceipt | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.evidence_origin == "production":
            if self.evidence is None or self.evidence.evidence_origin != "production":
                raise ValueError("production shadow report requires production evidence")
            for observation in (*self.legacy_observations, *self.isolated_observations):
                local_time = (
                    observation.event_time.astimezone(_SHANGHAI).timetz().replace(tzinfo=None)
                )
                in_morning = time(9, 25) <= local_time <= time(11, 30)
                in_afternoon = time(13, 0) <= local_time <= time(15, 0)
                if not in_morning and not in_afternoon:
                    raise ValueError("production shadow event is outside the market session")
        elif self.evidence is not None and self.evidence.evidence_origin != "test_fixture":
            raise ValueError("fixture shadow report cannot carry production evidence")
        if self.evidence is not None:
            local_trade_date = self.evidence.session_close_at.astimezone(_SHANGHAI).date()
            if local_trade_date != self.trade_date:
                raise ValueError("shadow evidence does not match report trade date")
            observed_bindings = {
                (
                    item.strategy_id,
                    item.strategy_version,
                    item.definition_fingerprint,
                    item.executable_fingerprint,
                )
                for item in (*self.legacy_observations, *self.isolated_observations)
            }
            snapshot_bindings = {
                (
                    item.binding.strategy_id,
                    item.binding.strategy_version,
                    item.binding.definition_fingerprint,
                    item.binding.executable_fingerprint,
                )
                for item in self.evidence.input_snapshots
            }
            if not observed_bindings.issubset(snapshot_bindings):
                raise ValueError("shadow input snapshots do not cover report bindings")
            snapshot_keys = {
                (
                    item.source,
                    item.binding.strategy_id,
                    item.binding.strategy_version,
                    item.binding.definition_fingerprint,
                    item.binding.executable_fingerprint,
                )
                for item in self.evidence.input_snapshots
            }
            for binding in snapshot_bindings:
                if not {("legacy", *binding), ("isolated", *binding)}.issubset(snapshot_keys):
                    raise ValueError("shadow evidence requires both source snapshots per binding")
            for snapshot in self.evidence.input_snapshots:
                scoped = tuple(
                    item
                    for item in (
                        self.legacy_observations
                        if snapshot.source == "legacy"
                        else self.isolated_observations
                    )
                    if (
                        item.strategy_id,
                        item.strategy_version,
                        item.definition_fingerprint,
                        item.executable_fingerprint,
                    )
                    == (
                        snapshot.binding.strategy_id,
                        snapshot.binding.strategy_version,
                        snapshot.binding.definition_fingerprint,
                        snapshot.binding.executable_fingerprint,
                    )
                )
                if snapshot.observation_set_id != _observation_set_id(scoped):
                    raise ValueError("shadow input snapshot does not bind its observations")
                if any(item.available_at > snapshot.captured_at for item in scoped):
                    raise ValueError("shadow observation was unavailable at source capture")
                if any(item.producer_commit != snapshot.producer_commit for item in scoped):
                    raise ValueError("shadow snapshot producer does not match its observations")
        derived = _derive_report(
            trade_date=self.trade_date,
            legacy=self.legacy_observations,
            isolated=self.isolated_observations,
            tolerance_microseconds=self.match_tolerance_microseconds,
        )
        for name in _DerivedReport.model_fields:
            if getattr(self, name) != getattr(derived, name):
                raise ValueError(f"shadow report derived field or match mismatch: {name}")
        if self.report_receipt is not None:
            _validate_shadow_report_receipt_binding(self)
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"report_id"}))
        if self.report_id is None:
            object.__setattr__(self, "report_id", expected)
        elif self.report_id != expected:
            raise ValueError("shadow report id does not match content")
        return self

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


def shadow_report_output_hash(report: ShadowSessionReport) -> str:
    """Hash the immutable report body, deliberately excluding its self-referential receipt."""

    return canonical_sha256(
        {
            "contract": "runtime-shadow-report-output/v1",
            "report": report.model_dump(
                mode="python",
                exclude={"report_id", "report_receipt"},
            ),
        }
    )


def shadow_evidence_source_generation_id(evidence: ShadowSessionEvidence) -> str:
    """Bind the report to every upstream source generation and raw input identity."""

    verified = ShadowSessionEvidence.model_validate(evidence)
    return canonical_sha256(
        {
            "contract": "runtime-shadow-source-generation/v1",
            "input_snapshots": tuple(
                sorted(
                    (
                        {
                            "source": item.source,
                            "source_id": item.source_id,
                            "binding": item.binding,
                            "raw_input_id": item.raw_input_id,
                            "upstream_snapshot_id": item.upstream_snapshot_id,
                            "completion_receipt_id": item.completion_receipt.receipt_id,
                            "runner_generation_id": item.completion_receipt.runner_generation_id,
                        }
                        for item in verified.input_snapshots
                    ),
                    key=lambda item: (
                        item["source"],
                        item["source_id"],
                        str(item["binding"].strategy_id),
                        int(item["binding"].strategy_version),
                    ),
                )
            ),
        }
    )


def _report_bindings(evidence: ShadowSessionEvidence) -> tuple[ShadowStrategyBinding, ...]:
    return tuple(
        sorted(
            {item.binding for item in evidence.input_snapshots},
            key=lambda item: (
                item.strategy_id,
                item.strategy_version,
                item.definition_fingerprint,
                item.executable_fingerprint,
            ),
        )
    )


def _validate_shadow_report_receipt_binding(report: ShadowSessionReport) -> None:
    receipt = report.report_receipt
    if receipt is None:
        return
    evidence = report.evidence
    if evidence is None or evidence.evidence_origin != "production":
        raise ValueError("shadow report receipt requires production evidence")
    claims = receipt.claims
    expected = {
        "trade_date": report.trade_date,
        "strategy_bindings": _report_bindings(evidence),
        "code_commit": evidence.producer_commit,
        "producer_commit": evidence.producer_commit,
        "producer_version": evidence.producer_version,
        "calendar_generation_id": evidence.calendar_authority_id,
        "source_generation_id": shadow_evidence_source_generation_id(evidence),
        "input_hash": evidence.evidence_id,
        "output_hash": shadow_report_output_hash(report),
    }
    for name, expected_value in expected.items():
        if getattr(claims, name) != expected_value:
            raise ValueError(f"shadow report receipt claim does not match report: {name}")


def attach_shadow_report_receipt(
    report: ShadowSessionReport,
    *,
    signer: Ed25519ShadowReceiptSigner,
    verifier: Ed25519ShadowReceiptKeyring,
    producer_service_id: str,
    producer_instance_id: str,
) -> ShadowSessionReport:
    """Attach a signed report receipt without exposing any signing material to Shadow."""

    verified = ShadowSessionReport.model_validate(report)
    evidence = verified.evidence
    if verified.evidence_origin != "production" or evidence is None:
        raise ValueError("only production shadow reports can receive a report receipt")
    if verified.report_receipt is not None:
        raise ValueError("shadow report already has a receipt")
    if not isinstance(signer, Ed25519ShadowReceiptSigner) or not isinstance(
        verifier,
        Ed25519ShadowReceiptKeyring,
    ):
        raise ValueError("production report receipt requires Ed25519 signing capabilities")
    if signer.key_id != verifier.active_key_id:
        raise ValueError("production report signer must use the active key id")
    receipt = signer.issue_report(
        ShadowReportReceiptClaims(
            trade_date=verified.trade_date,
            strategy_bindings=_report_bindings(evidence),
            producer_service_id=producer_service_id,
            producer_instance_id=producer_instance_id,
            code_commit=evidence.producer_commit,
            producer_commit=evidence.producer_commit,
            producer_version=evidence.producer_version,
            calendar_generation_id=evidence.calendar_authority_id,
            source_generation_id=shadow_evidence_source_generation_id(evidence),
            input_hash=evidence.evidence_id,
            output_hash=shadow_report_output_hash(verified),
        )
    )
    payload = verified.model_dump(
        mode="python",
        exclude={"report_id", "report_receipt"},
    )
    signed = ShadowSessionReport(**payload, report_receipt=receipt)
    if not verify_shadow_report_receipt(signed, verifier):
        raise ValueError("new shadow report receipt is untrusted")
    return signed


def verify_shadow_report_receipt(
    report: ShadowSessionReport,
    verifier: ShadowReportReceiptVerifier | None,
) -> bool:
    try:
        verified = ShadowSessionReport.model_validate(report)
    except (TypeError, ValueError):
        return False
    if verified.report_receipt is None or verifier is None:
        return False
    try:
        return bool(verifier.verify_report(verified.report_receipt))
    except (TypeError, ValueError, RecursionError):
        return False


class ShadowCalendarSelection(RuntimeContractModel):
    selection_id: Sha256 | None = None
    authority: MarketCalendarAuthority
    evaluated_at: AwareUtcDatetime
    maximum_sessions: int = Field(ge=10, le=20)
    selected_open_dates: tuple[date, ...]
    latest_closed_session: date

    @classmethod
    def create(
        cls,
        *,
        authority: MarketCalendarAuthority,
        evaluated_at,
        maximum_sessions: int,
    ) -> ShadowCalendarSelection:
        verified = MarketCalendarAuthority.model_validate(authority)
        observed = normalize_aware_utc(evaluated_at)
        closed = _closed_authoritative_dates(verified, observed)
        identity = {
            "authority": verified,
            "evaluated_at": observed,
            "maximum_sessions": maximum_sessions,
            "selected_open_dates": closed[-maximum_sessions:],
            "latest_closed_session": closed[-1],
        }
        return cls(**identity, selection_id=canonical_sha256(identity))

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        closed = _closed_authoritative_dates(self.authority, self.evaluated_at)
        if self.selected_open_dates != closed[-self.maximum_sessions :]:
            raise ValueError("selected open dates do not match calendar authority")
        if self.latest_closed_session != closed[-1]:
            raise ValueError("calendar latest closed session does not match authority")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"selection_id"}))
        if self.selection_id is None:
            object.__setattr__(self, "selection_id", expected)
        elif self.selection_id != expected:
            raise ValueError("calendar selection id does not match content")
        return self


class ShadowRetirementPolicy(RuntimeContractModel):
    required_consecutive_sessions: int = Field(default=10, ge=10, le=20)
    strategy_bindings: tuple[ShadowStrategyBinding, ...] = Field(min_length=1)
    match_tolerance_microseconds: Literal[60_000_000] = 60 * _MICROSECONDS_PER_SECOND
    minimum_legacy_recall_bps: int = Field(default=9500, ge=0, le=10_000)
    minimum_isolated_precision_bps: int = Field(default=9500, ge=0, le=10_000)
    minimum_isolated_latency_coverage_bps: int = Field(default=10_000, ge=0, le=10_000)
    maximum_isolated_p95_latency_microseconds: StrictInt = Field(
        default=10 * _MICROSECONDS_PER_SECOND,
        ge=0,
    )
    maximum_p95_latency_delta_microseconds: StrictInt = Field(
        default=10 * _MICROSECONDS_PER_SECOND,
        ge=0,
    )

    @field_validator("match_tolerance_microseconds", mode="before")
    @classmethod
    def validate_integer_match_tolerance(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("shadow match tolerance must use integer microseconds")
        return value

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        canonical = tuple(
            sorted(
                self.strategy_bindings,
                key=lambda item: (
                    item.strategy_id,
                    item.strategy_version,
                    item.definition_fingerprint,
                    item.executable_fingerprint,
                ),
            )
        )
        if len(canonical) != len(set(canonical)):
            raise ValueError("shadow strategy bindings must be unique")
        object.__setattr__(self, "strategy_bindings", canonical)
        return self


class ShadowStrategyRetirementResult(RuntimeContractModel):
    binding: ShadowStrategyBinding
    accepted_session_count: int = Field(ge=0)
    accepted_trade_dates: tuple[date, ...]
    reason_codes: tuple[str, ...]


class ShadowRetirementEvaluation(RuntimeContractModel):
    evaluation_id: Sha256 | None = None
    passed: bool
    policy: ShadowRetirementPolicy
    calendar_selection_id: Sha256
    evaluated_report_ids: tuple[Sha256, ...]
    accepted_session_count: int = Field(ge=0)
    strategy_results: tuple[ShadowStrategyRetirementResult, ...]
    reason_codes: tuple[str, ...]

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"evaluation_id"}))
        if self.evaluation_id is None:
            object.__setattr__(self, "evaluation_id", expected)
        elif self.evaluation_id != expected:
            raise ValueError("shadow retirement evaluation id does not match content")
        return self


def _timedelta_microseconds(value) -> int:
    return value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds


def _observation_set_id(observations: Sequence[ShadowObservation]) -> str:
    return canonical_sha256(
        {
            "contract": "runtime-shadow-observation-set/v2",
            "observation_ids": tuple(sorted(str(item.observation_id) for item in observations)),
        }
    )


def shadow_observation_set_id(observations: Iterable[ShadowObservation]) -> str:
    return _observation_set_id(
        tuple(ShadowObservation.model_validate(item) for item in observations)
    )


def shadow_session_boundaries(trade_date: date) -> tuple[datetime, datetime]:
    session_open = datetime.combine(trade_date, time(9, 25), tzinfo=_SHANGHAI).astimezone(
        ZoneInfo("UTC")
    )
    session_close = datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI).astimezone(
        ZoneInfo("UTC")
    )
    return session_open, session_close


def _semantic_key(item: ShadowObservation) -> tuple[str, int, str, str, str, str]:
    return (
        item.strategy_id,
        item.strategy_version,
        item.definition_fingerprint,
        item.executable_fingerprint,
        item.ts_code,
        item.action,
    )


def _entity_key(item: ShadowObservation) -> tuple[str, int, str, str, str]:
    return (
        item.strategy_id,
        item.strategy_version,
        item.definition_fingerprint,
        item.executable_fingerprint,
        item.ts_code,
    )


def _p95(values: Sequence[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)]


def _bps(numerator: int, denominator: int) -> int:
    if denominator == 0:
        return 10_000 if numerator == 0 else 0
    return numerator * 10_000 // denominator


def _maximum_time_matches(
    legacy_items: Sequence[ShadowObservation],
    isolated_items: Sequence[ShadowObservation],
    *,
    tolerance_microseconds: int,
) -> tuple[tuple[int, int], ...]:
    pairs: list[tuple[int, int]] = []
    legacy_index = isolated_index = 0
    while legacy_index < len(legacy_items) and isolated_index < len(isolated_items):
        legacy_time = legacy_items[legacy_index].event_time
        isolated_time = isolated_items[isolated_index].event_time
        signed_delta = _timedelta_microseconds(isolated_time - legacy_time)
        if signed_delta < -tolerance_microseconds:
            isolated_index += 1
        elif signed_delta > tolerance_microseconds:
            legacy_index += 1
        else:
            pairs.append((legacy_index, isolated_index))
            legacy_index += 1
            isolated_index += 1
    return tuple(pairs)


def _canonical_observations(
    observations: Iterable[ShadowObservation],
    *,
    source: Literal["legacy", "isolated"],
    trade_date: date,
) -> tuple[ShadowObservation, ...]:
    prepared_items: list[ShadowObservation] = []
    consumed_bytes = 0
    for raw_item in observations:
        if len(prepared_items) >= _MAX_REPORT_OBSERVATIONS:
            raise ValueError(f"{source} shadow observation budget exceeded")
        item = ShadowObservation.model_validate(raw_item)
        consumed_bytes += len(canonical_json_bytes(item.model_dump(mode="json")))
        if consumed_bytes > _MAX_REPORT_INPUT_BYTES:
            raise ValueError(f"{source} shadow byte budget exceeded")
        prepared_items.append(item)
    prepared = tuple(
        sorted(
            prepared_items,
            key=lambda item: (item.event_time, str(item.observation_id)),
        )
    )
    if any(item.source != source or item.trade_date != trade_date for item in prepared):
        raise ValueError(f"{source} shadow observations do not match the session")
    upstream_ids = [item.upstream_event_id for item in prepared]
    if len(upstream_ids) != len(set(upstream_ids)):
        raise ValueError(f"duplicate {source} upstream business event")
    semantic_events = [
        (
            item.strategy_id,
            item.strategy_version,
            item.definition_fingerprint,
            item.executable_fingerprint,
            item.ts_code,
            item.action,
            item.event_time,
        )
        for item in prepared
    ]
    if len(semantic_events) != len(set(semantic_events)):
        raise ValueError(f"duplicate {source} semantic business event")
    observation_ids = [item.observation_id for item in prepared]
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError(f"duplicate {source} shadow observation")
    return prepared


def _derive_report(
    *,
    trade_date: date,
    legacy: Iterable[ShadowObservation],
    isolated: Iterable[ShadowObservation],
    tolerance_microseconds: int,
) -> _DerivedReport:
    legacy_items = _canonical_observations(legacy, source="legacy", trade_date=trade_date)
    isolated_items = _canonical_observations(isolated, source="isolated", trade_date=trade_date)
    unmatched_isolated = set(range(len(isolated_items)))
    matched_legacy: set[int] = set()
    matches: list[ShadowObservationMatch] = []
    legacy_groups: dict[tuple[str, int, str, str, str, str], list[int]] = defaultdict(list)
    isolated_groups: dict[tuple[str, int, str, str, str, str], list[int]] = defaultdict(list)
    for index, item in enumerate(legacy_items):
        legacy_groups[_semantic_key(item)].append(index)
    for index, item in enumerate(isolated_items):
        isolated_groups[_semantic_key(item)].append(index)
    for semantic_key in sorted(set(legacy_groups).intersection(isolated_groups)):
        legacy_indices = legacy_groups[semantic_key]
        isolated_indices = isolated_groups[semantic_key]
        pairs = _maximum_time_matches(
            tuple(legacy_items[index] for index in legacy_indices),
            tuple(isolated_items[index] for index in isolated_indices),
            tolerance_microseconds=tolerance_microseconds,
        )
        for local_legacy, local_isolated in pairs:
            legacy_index = legacy_indices[local_legacy]
            isolated_index = isolated_indices[local_isolated]
            legacy_item = legacy_items[legacy_index]
            isolated_item = isolated_items[isolated_index]
            matched_legacy.add(legacy_index)
            unmatched_isolated.remove(isolated_index)
            availability_delta = None
            if (
                legacy_item.availability_basis == "observed_completion"
                and isolated_item.availability_basis == "observed_completion"
            ):
                availability_delta = abs(
                    _timedelta_microseconds(isolated_item.available_at - legacy_item.available_at)
                )
            matches.append(
                ShadowObservationMatch(
                    legacy_observation_id=str(legacy_item.observation_id),
                    isolated_observation_id=str(isolated_item.observation_id),
                    strategy_id=legacy_item.strategy_id,
                    strategy_version=legacy_item.strategy_version,
                    definition_fingerprint=legacy_item.definition_fingerprint,
                    executable_fingerprint=legacy_item.executable_fingerprint,
                    ts_code=legacy_item.ts_code,
                    action=legacy_item.action,
                    event_delta_microseconds=abs(
                        _timedelta_microseconds(isolated_item.event_time - legacy_item.event_time)
                    ),
                    availability_delta_microseconds=availability_delta,
                )
            )

    unmatched_legacy = set(range(len(legacy_items))) - matched_legacy
    discrepancies: list[ShadowObservationDiscrepancy] = []
    remaining_by_entity: dict[tuple[str, int, str, str, str], deque[int]] = defaultdict(deque)
    for isolated_index in sorted(unmatched_isolated):
        remaining_by_entity[_entity_key(isolated_items[isolated_index])].append(isolated_index)
    for legacy_index in sorted(unmatched_legacy):
        legacy_item = legacy_items[legacy_index]
        candidates = remaining_by_entity[_entity_key(legacy_item)]
        while candidates and candidates[0] not in unmatched_isolated:
            candidates.popleft()
        if not candidates:
            discrepancies.append(
                ShadowObservationDiscrepancy(
                    legacy_observation_id=str(legacy_item.observation_id),
                    strategy_id=legacy_item.strategy_id,
                    strategy_version=legacy_item.strategy_version,
                    definition_fingerprint=legacy_item.definition_fingerprint,
                    executable_fingerprint=legacy_item.executable_fingerprint,
                    ts_code=legacy_item.ts_code,
                    reason="legacy_only",
                )
            )
            continue
        isolated_index = candidates.popleft()
        isolated_item = isolated_items[isolated_index]
        unmatched_isolated.remove(isolated_index)
        discrepancies.append(
            ShadowObservationDiscrepancy(
                legacy_observation_id=str(legacy_item.observation_id),
                isolated_observation_id=str(isolated_item.observation_id),
                strategy_id=legacy_item.strategy_id,
                strategy_version=legacy_item.strategy_version,
                definition_fingerprint=legacy_item.definition_fingerprint,
                executable_fingerprint=legacy_item.executable_fingerprint,
                ts_code=legacy_item.ts_code,
                reason=(
                    "action_mismatch"
                    if isolated_item.action != legacy_item.action
                    else "outside_time_tolerance"
                ),
            )
        )
    for isolated_index in sorted(unmatched_isolated):
        item = isolated_items[isolated_index]
        discrepancies.append(
            ShadowObservationDiscrepancy(
                isolated_observation_id=str(item.observation_id),
                strategy_id=item.strategy_id,
                strategy_version=item.strategy_version,
                definition_fingerprint=item.definition_fingerprint,
                executable_fingerprint=item.executable_fingerprint,
                ts_code=item.ts_code,
                reason="isolated_only",
            )
        )

    canonical_matches = tuple(
        sorted(
            matches,
            key=lambda item: (
                item.strategy_id,
                item.strategy_version,
                item.definition_fingerprint,
                item.executable_fingerprint,
                item.ts_code,
                item.action,
            ),
        )
    )
    comparable_deltas = tuple(
        item.availability_delta_microseconds
        for item in canonical_matches
        if item.availability_delta_microseconds is not None
    )
    isolated_latencies = tuple(
        _timedelta_microseconds(item.available_at - item.event_time)
        for item in isolated_items
        if item.availability_basis == "observed_completion"
    )
    return _DerivedReport(
        legacy_observation_set_id=_observation_set_id(legacy_items),
        isolated_observation_set_id=_observation_set_id(isolated_items),
        legacy_count=len(legacy_items),
        isolated_count=len(isolated_items),
        matched_count=len(canonical_matches),
        legacy_only_count=len(legacy_items) - len(canonical_matches),
        isolated_only_count=len(isolated_items) - len(canonical_matches),
        legacy_recall_bps=_bps(len(canonical_matches), len(legacy_items)),
        isolated_precision_bps=_bps(len(canonical_matches), len(isolated_items)),
        comparable_latency_match_count=len(comparable_deltas),
        p95_latency_delta_microseconds=(_p95(comparable_deltas) if comparable_deltas else None),
        isolated_latency_measurement_count=len(isolated_latencies),
        isolated_latency_coverage_bps=_bps(len(isolated_latencies), len(isolated_items)),
        isolated_p95_latency_microseconds=_p95(isolated_latencies),
        matches=canonical_matches,
        discrepancies=tuple(
            sorted(
                discrepancies,
                key=lambda item: (
                    item.strategy_id,
                    item.strategy_version,
                    item.definition_fingerprint,
                    item.executable_fingerprint,
                    item.ts_code,
                    item.reason,
                    item.legacy_observation_id or "",
                    item.isolated_observation_id or "",
                ),
            )
        ),
    )


def build_shadow_session_report(
    *,
    trade_date: date,
    legacy: Iterable[ShadowObservation],
    isolated: Iterable[ShadowObservation],
    match_tolerance_microseconds: int,
    evidence: ShadowSessionEvidence | None = None,
    attestation_verifier: CompletionAttestationVerifier | None = None,
) -> ShadowSessionReport:
    if evidence is not None and evidence.evidence_origin == "production":
        if not isinstance(attestation_verifier, Ed25519CompletionAttestationKeyring):
            raise ValueError("production shadow report requires an Ed25519 completion keyring")
        for snapshot in evidence.input_snapshots:
            if snapshot.source != "isolated":
                continue
            receipt = snapshot.completion_receipt
            if (
                receipt.calendar_generation_id != evidence.calendar_authority_id
                or receipt.completion_attestation is None
                or receipt.completion_attestation.claims.calendar_generation_id
                != evidence.calendar_authority_id
            ):
                raise ValueError("production shadow report receipt calendar lineage mismatch")
            if not verify_completion_attestation(receipt, attestation_verifier):
                raise ValueError("production shadow report has an untrusted completion attestation")
            attestation = receipt.completion_attestation
            if attestation is None:
                raise ValueError("production shadow report completion attestation is missing")
            if attestation.key_id != attestation_verifier.active_key_id:
                raise ValueError("new production completion attestation must use the active key id")
            claims = attestation.claims
            if (
                claims.strategy_id != snapshot.binding.strategy_id
                or claims.strategy_version != snapshot.binding.strategy_version
                or claims.strategy_registration_fingerprint
                != snapshot.binding.definition_fingerprint
                or claims.executable_fingerprint != snapshot.binding.executable_fingerprint
            ):
                raise ValueError("production shadow report completion attestation binding mismatch")
    legacy_items = _canonical_observations(legacy, source="legacy", trade_date=trade_date)
    isolated_items = _canonical_observations(isolated, source="isolated", trade_date=trade_date)
    derived = _derive_report(
        trade_date=trade_date,
        legacy=legacy_items,
        isolated=isolated_items,
        tolerance_microseconds=match_tolerance_microseconds,
    )
    return ShadowSessionReport(
        evidence_origin=(evidence.evidence_origin if evidence is not None else "test_fixture"),
        evidence=evidence,
        trade_date=trade_date,
        match_tolerance_microseconds=match_tolerance_microseconds,
        legacy_observations=legacy_items,
        isolated_observations=isolated_items,
        **derived.model_dump(mode="python"),
    )


def _strategy_metrics(
    report: ShadowSessionReport,
    binding: ShadowStrategyBinding,
) -> _DerivedReport:
    scope = (
        binding.strategy_id,
        binding.strategy_version,
        binding.definition_fingerprint,
        binding.executable_fingerprint,
    )
    return _derive_report(
        trade_date=report.trade_date,
        legacy=(
            item
            for item in report.legacy_observations
            if (
                item.strategy_id,
                item.strategy_version,
                item.definition_fingerprint,
                item.executable_fingerprint,
            )
            == scope
        ),
        isolated=(
            item
            for item in report.isolated_observations
            if (
                item.strategy_id,
                item.strategy_version,
                item.definition_fingerprint,
                item.executable_fingerprint,
            )
            == scope
        ),
        tolerance_microseconds=report.match_tolerance_microseconds,
    )


def _ratio_meets(numerator: int, denominator: int, minimum_bps: int) -> bool:
    return denominator > 0 and numerator * 10_000 >= minimum_bps * denominator


def evaluate_shadow_retirement_gate(
    reports: Iterable[ShadowSessionReport],
    *,
    calendar_selection: ShadowCalendarSelection,
    policy: ShadowRetirementPolicy,
    attestation_verifier: CompletionAttestationVerifier | None = None,
    report_receipt_verifier: ShadowReportReceiptVerifier | None = None,
) -> ShadowRetirementEvaluation:
    calendar_selection = ShadowCalendarSelection.model_validate(
        calendar_selection.model_dump(mode="python")
    )
    policy = ShadowRetirementPolicy.model_validate(policy.model_dump(mode="python"))
    unique_reports: dict[str, ShadowSessionReport] = {}
    for item in reports:
        report = ShadowSessionReport.model_validate(item)
        unique_reports.setdefault(str(report.report_id), report)
    report_items = tuple(unique_reports.values())
    by_date: dict[date, ShadowSessionReport] = {}
    selected = set(calendar_selection.selected_open_dates)
    for report in report_items:
        if report.trade_date in by_date:
            raise ValueError("duplicate shadow session report")
        if report.trade_date not in selected:
            raise ValueError("shadow session is absent from the selected authoritative calendar")
        if report.match_tolerance_microseconds != policy.match_tolerance_microseconds:
            raise ValueError("shadow report tolerance conflicts with retirement policy")
        by_date[report.trade_date] = report
    allowed_bindings = {
        (
            item.strategy_id,
            item.strategy_version,
            item.definition_fingerprint,
            item.executable_fingerprint,
        )
        for item in policy.strategy_bindings
    }
    observed_bindings = {
        (
            item.strategy_id,
            item.strategy_version,
            item.definition_fingerprint,
            item.executable_fingerprint,
        )
        for report in report_items
        for item in (*report.legacy_observations, *report.isolated_observations)
    }
    if not observed_bindings.issubset(allowed_bindings):
        raise ValueError("shadow report contains an unexpected strategy binding")
    for report in report_items:
        if report.evidence is None:
            continue
        snapshot_bindings = {
            (
                item.binding.strategy_id,
                item.binding.strategy_version,
                item.binding.definition_fingerprint,
                item.binding.executable_fingerprint,
            )
            for item in report.evidence.input_snapshots
        }
        if not snapshot_bindings.issubset(allowed_bindings):
            raise ValueError("shadow evidence bindings conflict with retirement policy")
    required_dates = calendar_selection.selected_open_dates[-policy.required_consecutive_sessions :]
    strategy_results: list[ShadowStrategyRetirementResult] = []
    for binding in policy.strategy_bindings:
        accepted: list[date] = []
        reasons: list[str] = []
        if len(required_dates) < policy.required_consecutive_sessions:
            reasons.append("insufficient_authoritative_sessions")
        else:
            for trade_date in reversed(required_dates):
                report = by_date.get(trade_date)
                if report is None:
                    reasons.append("authoritative_session_missing")
                    break
                if report.evidence_origin != "production" or report.evidence is None:
                    reasons.append("non_production_evidence")
                    break
                if not isinstance(
                    attestation_verifier,
                    Ed25519CompletionAttestationKeyring,
                ) or not isinstance(
                    report_receipt_verifier,
                    Ed25519ShadowReceiptKeyring,
                ):
                    reasons.append("production_ed25519_verifier_required")
                    break
                if not verify_shadow_report_receipt(report, report_receipt_verifier):
                    reasons.append("report_receipt_unverified")
                    break
                if report.evidence.calendar_authority_id != str(
                    calendar_selection.authority.content_sha256
                ):
                    reasons.append("calendar_identity_mismatch")
                    break
                if any(
                    snapshot.source == "isolated"
                    and snapshot.completion_receipt.calendar_generation_id
                    != report.evidence.calendar_authority_id
                    for snapshot in report.evidence.input_snapshots
                ):
                    reasons.append("completion_calendar_identity_mismatch")
                    break
                isolated_snapshots = tuple(
                    snapshot
                    for snapshot in report.evidence.input_snapshots
                    if snapshot.source == "isolated" and snapshot.binding == binding
                )
                if len(isolated_snapshots) != 1:
                    reasons.append("completion_attestation_binding_missing")
                    break
                isolated_receipt = isolated_snapshots[0].completion_receipt
                if not verify_completion_attestation(
                    isolated_receipt,
                    attestation_verifier,
                ):
                    reasons.append("completion_attestation_unverified")
                    break
                attestation = isolated_receipt.completion_attestation
                if attestation is None:
                    reasons.append("completion_attestation_unverified")
                    break
                claims = attestation.claims
                if (
                    claims.strategy_id != binding.strategy_id
                    or claims.strategy_version != binding.strategy_version
                    or claims.strategy_registration_fingerprint != binding.definition_fingerprint
                    or claims.executable_fingerprint != binding.executable_fingerprint
                ):
                    reasons.append("completion_attestation_binding_mismatch")
                    break
                if calendar_selection.authority.generated_at > report.evidence.evaluation_cutoff:
                    reasons.append("calendar_unavailable_at_evidence_cutoff")
                    break
                if report.evidence.evaluation_cutoff > calendar_selection.evaluated_at:
                    reasons.append("evidence_after_evaluation_cutoff")
                    break
                metrics = _strategy_metrics(report, binding)
                if metrics.legacy_count == 0 or metrics.isolated_count == 0:
                    reasons.append("strategy_observations_missing")
                    break
                if not _ratio_meets(
                    metrics.matched_count,
                    metrics.legacy_count,
                    policy.minimum_legacy_recall_bps,
                ):
                    reasons.append("legacy_recall_below_threshold")
                    break
                if not _ratio_meets(
                    metrics.matched_count,
                    metrics.isolated_count,
                    policy.minimum_isolated_precision_bps,
                ):
                    reasons.append("isolated_precision_below_threshold")
                    break
                if not _ratio_meets(
                    metrics.isolated_latency_measurement_count,
                    metrics.isolated_count,
                    policy.minimum_isolated_latency_coverage_bps,
                ):
                    reasons.append("isolated_latency_coverage_below_threshold")
                    break
                if (
                    metrics.isolated_p95_latency_microseconds
                    > policy.maximum_isolated_p95_latency_microseconds
                ):
                    reasons.append("isolated_latency_above_threshold")
                    break
                if (
                    metrics.p95_latency_delta_microseconds is not None
                    and metrics.p95_latency_delta_microseconds
                    > policy.maximum_p95_latency_delta_microseconds
                ):
                    reasons.append("latency_delta_above_threshold")
                    break
                accepted.append(trade_date)
        strategy_results.append(
            ShadowStrategyRetirementResult(
                binding=binding,
                accepted_session_count=len(accepted),
                accepted_trade_dates=tuple(reversed(accepted)),
                reason_codes=tuple(reasons),
            )
        )
    accepted_count = min((item.accepted_session_count for item in strategy_results), default=0)
    passed = accepted_count >= policy.required_consecutive_sessions and all(
        not item.reason_codes for item in strategy_results
    )
    evaluation_reasons = {reason for item in strategy_results for reason in item.reason_codes}
    reasons = (
        () if passed else tuple(sorted({"insufficient_consecutive_sessions", *evaluation_reasons}))
    )
    return ShadowRetirementEvaluation(
        passed=passed,
        policy=policy,
        calendar_selection_id=str(calendar_selection.selection_id),
        evaluated_report_ids=tuple(
            str(item.report_id) for item in sorted(report_items, key=lambda item: item.trade_date)
        ),
        accepted_session_count=accepted_count,
        strategy_results=tuple(strategy_results),
        reason_codes=reasons,
    )


def _normalized_absolute(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.abspath(candidate))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("shadow path must be absolute and normalized")
    return candidate


def _safe_directory(path: Path) -> Path:
    candidate = _normalized_absolute(path)
    descriptor = os.open(candidate.anchor, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        for component in candidate.parts[1:]:
            try:
                observed = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                observed = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise ValueError("shadow path contains a symlink or unsafe component")
            if observed.st_uid not in {0, os.geteuid()}:
                raise ValueError("shadow directory is not owned by the runtime user")
            child = os.open(
                component,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)
    return candidate


def _open_existing_directory(path: Path) -> int:
    candidate = _normalized_absolute(path)
    descriptor = os.open(
        candidate.anchor,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        for component in candidate.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except OSError as exc:
        os.close(descriptor)
        raise ValueError("shadow path contains a symlink or unsafe component") from exc


def _read_regular(path: Path, *, label: str, maximum_bytes: int = _MAX_REPORT_BYTES) -> bytes:
    candidate = _normalized_absolute(path)
    parent = _open_existing_directory(candidate.parent)
    descriptor = -1
    try:
        try:
            observed = os.stat(candidate.name, dir_fd=parent, follow_symlinks=False)
            descriptor = os.open(
                candidate.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent,
            )
        except OSError as exc:
            raise ValueError(f"{label} is unavailable or unsafe") from exc
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_size > maximum_bytes
            or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
        ):
            raise ValueError(f"{label} has an unsafe identity")
        chunks: list[bytes] = []
        consumed = 0
        while consumed <= maximum_bytes:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - consumed))
            if not chunk:
                break
            chunks.append(chunk)
            consumed += len(chunk)
        if consumed > maximum_bytes:
            raise ValueError(f"{label} exceeds its byte budget")
        completed = os.fstat(descriptor)
        if (
            completed.st_size != opened.st_size
            or completed.st_mtime_ns != opened.st_mtime_ns
            or completed.st_ctime_ns != opened.st_ctime_ns
            or completed.st_nlink != 1
        ):
            raise ValueError(f"{label} changed while reading")
        return b"".join(chunks)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent)


def load_shadow_session_report(
    path: Path,
    *,
    expected_report_id: str,
) -> ShadowSessionReport:
    candidate = _normalized_absolute(path)
    payload = _read_regular(candidate, label="shadow report")
    _preflight_report_json(payload)
    try:
        decoded = strict_canonical_json_loads(payload)
        report = ShadowSessionReport.model_validate(decoded)
    except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("shadow report content is invalid") from exc
    if report.report_id != expected_report_id:
        raise ValueError("shadow report identity does not match the expected report")
    if candidate.name != f"{expected_report_id}.json":
        raise ValueError("shadow report filename does not match its identity")
    return report


def _read_regular_at(
    directory_fd: int,
    name: str,
    *,
    label: str,
    allowed_links: frozenset[int],
    maximum_bytes: int = _MAX_REPORT_BYTES,
) -> bytes | None:
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError(f"{label} is unavailable or unsafe") from exc
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink not in allowed_links
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_size > maximum_bytes
        ):
            raise ValueError(f"{label} has an unsafe identity")
        chunks: list[bytes] = []
        remaining = observed.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError(f"{label} ended before its declared size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError(f"{label} grew while reading")
        completed = os.fstat(descriptor)
        if (
            completed.st_size != observed.st_size
            or completed.st_mtime_ns != observed.st_mtime_ns
            or completed.st_ctime_ns != observed.st_ctime_ns
        ):
            raise ValueError(f"{label} changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _write_exclusive_at(directory_fd: int, name: str, payload: bytes) -> None:
    descriptor = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=directory_fd,
    )
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("shadow publication write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _decode_report_payload(payload: bytes, *, expected_report_id: str) -> ShadowSessionReport:
    _preflight_report_json(payload)
    try:
        report = ShadowSessionReport.model_validate(strict_canonical_json_loads(payload))
    except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("shadow report content is invalid") from exc
    if report.report_id != expected_report_id:
        raise ValueError("shadow report identity does not match the expected report")
    return report


def publish_shadow_session_report(
    root: Path,
    report: ShadowSessionReport,
    *,
    fault_hook: ShadowPublicationFaultHook | None = None,
) -> Path:
    validated = ShadowSessionReport.model_validate(report)
    report_id = str(validated.report_id)
    target_name = f"{report_id}.json"
    temporary_name = f".shadow-{report_id}.tmp"
    intent_name = f".publish-{report_id}.intent.json"
    session_claim_name = ".session-report-claim.json"
    payload = validated.canonical_bytes()
    if len(payload) > _MAX_REPORT_BYTES:
        raise ValueError("shadow report exceeds the serialized byte budget")
    session = _safe_directory(_safe_directory(root) / validated.trade_date.isoformat())
    session_claim_payload = canonical_json_bytes(
        {
            "contract": "shadow-session-report-claim/v1",
            "report_id": report_id,
            "trade_date": validated.trade_date.isoformat(),
        }
    )
    intent_payload = canonical_json_bytes(
        {
            "contract": "shadow-report-publish-intent/v1",
            "report_id": report_id,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "temporary": temporary_name,
            "target": target_name,
        }
    )
    directory_fd = _open_existing_directory(session)
    try:
        directory = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid():
            raise ValueError("shadow report session directory is unsafe")
        session_claim = _read_regular_at(
            directory_fd,
            session_claim_name,
            label="shadow session report claim",
            allowed_links=frozenset({1}),
            maximum_bytes=16 * 1024,
        )
        if session_claim is None:
            try:
                _write_exclusive_at(directory_fd, session_claim_name, session_claim_payload)
                os.fsync(directory_fd)
                session_claim = session_claim_payload
            except FileExistsError:
                session_claim = _read_regular_at(
                    directory_fd,
                    session_claim_name,
                    label="shadow session report claim",
                    allowed_links=frozenset({1}),
                    maximum_bytes=16 * 1024,
                )
        if session_claim != session_claim_payload:
            raise ValueError("shadow session conflicts with an existing immutable report")
        conflicting_reports = tuple(
            name
            for name in os.listdir(directory_fd)
            if name.endswith(".json") and not name.startswith(".") and name != target_name
        )
        if conflicting_reports:
            raise ValueError("shadow session conflicts with an existing immutable report")
        existing_intent = _read_regular_at(
            directory_fd,
            intent_name,
            label="shadow publication intent",
            allowed_links=frozenset({1}),
            maximum_bytes=16 * 1024,
        )
        target_payload = _read_regular_at(
            directory_fd,
            target_name,
            label="shadow report",
            allowed_links=frozenset({1, 2}) if existing_intent is not None else frozenset({1}),
        )
        temporary_payload = _read_regular_at(
            directory_fd,
            temporary_name,
            label="shadow publication temporary",
            allowed_links=frozenset({1, 2}),
        )

        if existing_intent is None:
            if temporary_payload is not None:
                raise ValueError("shadow temporary exists without a verified publish intent")
            if target_payload is not None:
                if (
                    _decode_report_payload(target_payload, expected_report_id=report_id)
                    != validated
                ):
                    raise ValueError("immutable shadow report conflicts with existing content")
                return session / target_name
            _write_exclusive_at(directory_fd, intent_name, intent_payload)
            os.fsync(directory_fd)
            existing_intent = intent_payload
            if fault_hook is not None:
                fault_hook("after_intent_write")
        elif existing_intent != intent_payload:
            if target_payload is not None or temporary_payload is not None:
                raise ValueError("shadow publication intent conflicts with report content")
            os.unlink(intent_name, dir_fd=directory_fd)
            _write_exclusive_at(directory_fd, intent_name, intent_payload)
            os.fsync(directory_fd)
            existing_intent = intent_payload
            if fault_hook is not None:
                fault_hook("after_intent_write")

        if target_payload is not None:
            if target_payload != payload:
                raise ValueError("interrupted shadow publication content conflicts")
            if temporary_payload is not None:
                if temporary_payload != payload:
                    raise ValueError("interrupted shadow publication content conflicts")
                target_stat = os.stat(target_name, dir_fd=directory_fd, follow_symlinks=False)
                temporary_stat = os.stat(
                    temporary_name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if (target_stat.st_dev, target_stat.st_ino) != (
                    temporary_stat.st_dev,
                    temporary_stat.st_ino,
                ):
                    raise ValueError("interrupted shadow publication link identity conflicts")
                os.unlink(temporary_name, dir_fd=directory_fd)
            os.unlink(intent_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
            if (
                _read_regular_at(
                    directory_fd,
                    target_name,
                    label="shadow report",
                    allowed_links=frozenset({1}),
                )
                != payload
            ):
                raise ValueError("recovered shadow report is not immutable")
            return session / target_name

        if temporary_payload is None:
            _write_exclusive_at(directory_fd, temporary_name, payload)
            os.fsync(directory_fd)
            if fault_hook is not None:
                fault_hook("after_temporary_write")
        elif temporary_payload != payload:
            os.unlink(temporary_name, dir_fd=directory_fd)
            _write_exclusive_at(directory_fd, temporary_name, payload)
            os.fsync(directory_fd)
            if fault_hook is not None:
                fault_hook("after_temporary_write")
        try:
            os.link(
                temporary_name,
                target_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            existing = _read_regular_at(
                directory_fd,
                target_name,
                label="shadow report",
                allowed_links=frozenset({1, 2}),
            )
            if existing != payload:
                raise ValueError(
                    "immutable shadow report conflicts with existing content"
                ) from None
        os.fsync(directory_fd)
        if fault_hook is not None:
            fault_hook("after_link")
        os.unlink(temporary_name, dir_fd=directory_fd)
        os.unlink(intent_name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        if (
            _read_regular_at(
                directory_fd,
                target_name,
                label="shadow report",
                allowed_links=frozenset({1}),
            )
            != payload
        ):
            raise ValueError("published shadow report is not immutable")
        if fault_hook is not None:
            fault_hook("after_receipt_write")
        return session / target_name
    finally:
        os.close(directory_fd)


def recover_shadow_session_publication(root: Path, *, trade_date: date) -> Path | None:
    """Finish a verified interrupted hard-link publication before normal loading."""

    report_root = _normalized_absolute(root)
    session = report_root / trade_date.isoformat()
    try:
        candidates = tuple(
            path
            for path in session.iterdir()
            if path.name.endswith(".json") and not path.name.startswith(".")
        )
    except FileNotFoundError:
        return None
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError("shadow session must contain exactly one report")
    target = candidates[0]
    expected_report_id = target.name.removesuffix(".json")
    if len(expected_report_id) != 64 or any(
        character not in "0123456789abcdef" for character in expected_report_id
    ):
        raise ValueError("shadow report filename has an invalid identity")
    intent_name = f".publish-{expected_report_id}.intent.json"
    temporary_name = f".shadow-{expected_report_id}.tmp"
    directory_fd = _open_existing_directory(session)
    try:
        intent_payload = _read_regular_at(
            directory_fd,
            intent_name,
            label="shadow publication intent",
            allowed_links=frozenset({1}),
            maximum_bytes=16 * 1024,
        )
        if intent_payload is None:
            return target
        target_payload = _read_regular_at(
            directory_fd,
            target.name,
            label="shadow report",
            allowed_links=frozenset({1, 2}),
        )
        if target_payload is None:
            raise ValueError("shadow publication intent has no durable report")
        try:
            intent = strict_canonical_json_loads(intent_payload)
        except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
            raise ValueError("shadow publication intent is invalid") from exc
        expected_intent = {
            "contract": "shadow-report-publish-intent/v1",
            "report_id": expected_report_id,
            "payload_sha256": hashlib.sha256(target_payload).hexdigest(),
            "temporary": temporary_name,
            "target": target.name,
        }
        if intent != expected_intent:
            raise ValueError("shadow publication intent conflicts with report content")
        report = _decode_report_payload(
            target_payload,
            expected_report_id=expected_report_id,
        )
    finally:
        os.close(directory_fd)
    return publish_shadow_session_report(report_root, report)


__all__ = [
    "CompletionAttestation",
    "CompletionAttestationClaims",
    "CompletionAttestationSigner",
    "CompletionAttestationSigningClient",
    "CompletionAttestationVerifier",
    "Ed25519CompletionAttestationKeyring",
    "Ed25519CompletionAttestationSigner",
    "Ed25519ShadowReceiptKeyring",
    "Ed25519ShadowReceiptSigner",
    "HmacCompletionAttestationAuthority",
    "SecureShadowSigningClient",
    "ShadowCalendarSelection",
    "ShadowInputSnapshotIdentity",
    "ShadowObservation",
    "ShadowObservationDiscrepancy",
    "ShadowObservationMatch",
    "ShadowRetirementEvaluation",
    "ShadowRetirementPolicy",
    "ShadowSessionReport",
    "ShadowSessionEvidence",
    "ShadowStrategyBinding",
    "ShadowStrategyRetirementResult",
    "ShadowReportReceipt",
    "ShadowReportReceiptClaims",
    "ShadowReportReceiptSigner",
    "ShadowReportReceiptVerifier",
    "ShadowSigningRequest",
    "ShadowSigningResponse",
    "attach_shadow_report_receipt",
    "build_shadow_session_report",
    "evaluate_shadow_retirement_gate",
    "load_shadow_session_report",
    "publish_shadow_session_report",
    "recover_shadow_session_publication",
    "shadow_completion_receipt_body_sha256",
    "shadow_observation_set_id",
    "shadow_evidence_source_generation_id",
    "shadow_report_output_hash",
    "shadow_session_boundaries",
    "verify_completion_attestation",
    "verify_shadow_report_receipt",
]
