"""Strict, verify-only Ed25519 support for formal in-process trust checks."""

from __future__ import annotations

import base64
import hashlib
from functools import lru_cache

_FIELD = 2**255 - 19
_ORDER = 2**252 + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _FIELD - 2, _FIELD)) % _FIELD
_SQRT_MINUS_ONE = pow(2, (_FIELD - 1) // 4, _FIELD)
_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")
_IDENTITY = (0, 1, 1, 0)
_Point = tuple[int, int, int, int]


class Ed25519VerificationError(ValueError):
    """An Ed25519 public key or encoded point is not strict and canonical."""


def _recover_x(y: int, sign: int) -> int:
    numerator = (y * y - 1) % _FIELD
    denominator = (_D * y * y + 1) % _FIELD
    x_squared = numerator * pow(denominator, _FIELD - 2, _FIELD) % _FIELD
    x = pow(x_squared, (_FIELD + 3) // 8, _FIELD)
    if (x * x - x_squared) % _FIELD:
        x = x * _SQRT_MINUS_ONE % _FIELD
    if (x * x - x_squared) % _FIELD:
        raise Ed25519VerificationError("encoded Ed25519 point is not on the curve")
    if x == 0 and sign:
        raise Ed25519VerificationError("encoded Ed25519 point is not canonical")
    if x & 1 != sign:
        x = _FIELD - x
    return x


def _decode_point(encoded: bytes) -> _Point:
    if len(encoded) != 32:
        raise Ed25519VerificationError("encoded Ed25519 point has invalid length")
    value = int.from_bytes(encoded, "little")
    y = value & ((1 << 255) - 1)
    if y >= _FIELD:
        raise Ed25519VerificationError("encoded Ed25519 point is not canonical")
    x = _recover_x(y, value >> 255)
    return (x, y, 1, x * y % _FIELD)


def _add(left: _Point, right: _Point) -> _Point:
    x1, y1, z1, t1 = left
    x2, y2, z2, t2 = right
    a = (y1 - x1) * (y2 - x2) % _FIELD
    b = (y1 + x1) * (y2 + x2) % _FIELD
    c = 2 * _D * t1 * t2 % _FIELD
    d = 2 * z1 * z2 % _FIELD
    e = b - a
    f = d - c
    g = d + c
    h = b + a
    return (e * f % _FIELD, g * h % _FIELD, f * g % _FIELD, e * h % _FIELD)


def _multiply(scalar: int, point: _Point) -> _Point:
    result = _IDENTITY
    addend = point
    while scalar:
        if scalar & 1:
            result = _add(result, addend)
        addend = _add(addend, addend)
        scalar >>= 1
    return result


def _equal(left: _Point, right: _Point) -> bool:
    return (left[0] * right[2] - right[0] * left[2]) % _FIELD == 0 and (
        left[1] * right[2] - right[1] * left[2]
    ) % _FIELD == 0


def _is_identity(point: _Point) -> bool:
    return _equal(point, _IDENTITY)


_BASE_Y = 4 * pow(5, _FIELD - 2, _FIELD) % _FIELD
_BASE_X = _recover_x(_BASE_Y, 0)
_BASE = (_BASE_X, _BASE_Y, 1, _BASE_X * _BASE_Y % _FIELD)


@lru_cache(maxsize=256)
def ed25519_public_key_der(public_key_pem: bytes) -> bytes:
    """Return the exact RFC 8410 SubjectPublicKeyInfo for one Ed25519 key."""

    try:
        lines = public_key_pem.splitlines()
        if (
            len(lines) != 3
            or lines[0] != b"-----BEGIN PUBLIC KEY-----"
            or lines[2] != b"-----END PUBLIC KEY-----"
        ):
            raise Ed25519VerificationError("public key is not canonical Ed25519 SPKI PEM")
        der = base64.b64decode(lines[1], validate=True)
    except (TypeError, ValueError) as exc:
        raise Ed25519VerificationError("public key is not canonical Ed25519 SPKI PEM") from exc
    if (
        len(der) != len(_SPKI_PREFIX) + 32
        or not der.startswith(_SPKI_PREFIX)
        or base64.b64encode(der) != lines[1]
    ):
        raise Ed25519VerificationError("public key is not canonical Ed25519 SPKI PEM")
    point = _decode_point(der[len(_SPKI_PREFIX) :])
    if _is_identity(point) or not _is_identity(_multiply(_ORDER, point)):
        raise Ed25519VerificationError("public key is not in the Ed25519 prime-order subgroup")
    return der


def verify_ed25519_signature(
    *,
    public_key_pem: bytes,
    message: bytes,
    signature: bytes,
) -> bool:
    """Verify a strict RFC 8032 Ed25519 signature without external processes."""

    if len(signature) != 64:
        return False
    try:
        public_key = ed25519_public_key_der(public_key_pem)[len(_SPKI_PREFIX) :]
        public_point = _decode_point(public_key)
        encoded_r = signature[:32]
        r_point = _decode_point(encoded_r)
        scalar = int.from_bytes(signature[32:], "little")
        if scalar >= _ORDER:
            return False
        if _is_identity(r_point) or not _is_identity(_multiply(_ORDER, r_point)):
            return False
        challenge = (
            int.from_bytes(
                hashlib.sha512(encoded_r + public_key + message).digest(),
                "little",
            )
            % _ORDER
        )
        return _equal(
            _multiply(scalar, _BASE),
            _add(r_point, _multiply(challenge, public_point)),
        )
    except Ed25519VerificationError:
        return False


__all__ = [
    "Ed25519VerificationError",
    "ed25519_public_key_der",
    "verify_ed25519_signature",
]
