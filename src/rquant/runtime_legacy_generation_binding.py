"""The one document that ties an authority generation to the legacy generation it came from.

Two generation id namespaces meet in a kind-backed role and never agree by construction
(#207, and #187 for the narrow reading of it):

* the **authority** generation id is `sha256(<generation>/full-manifest.json)`, derived by
  the wrapper from the root-owned chain slot and forwarded as `--expected-generation`
  (`runtime_exec_wrapper/_verify.py`);
* the **legacy** generation id is the deployment-bundle hash that
  `<runtime root>/current -> generations/<64 hex>` names
  (`runtime_deployment_bundle._publish_current`).

`load_runtime_schema_service_bindings` asks for the legacy one and used to be handed the
authority one, so once Route A restores `data/runtime/current` every kind-backed role
failed closed with `runtime schema service generation is not current`.

Handing it the legacy id alone would drop a binding rather than fix one: the role would
then trust whatever `current` happened to point at. So `runtime-authority-stage` writes
this document into the generation it stages, naming the legacy runtime root and generation
it copied the service manifests out of, and the role refuses unless the pointer still
resolves to that same generation. The document is a manifested file, which means its
sha256 is inside the generation's full manifest, whose sha256 *is* the authority
generation id, and which the wrapper hashes against the chain slot and then verifies on
disk entry by entry (`_verify.verify_code_identity`) before the role process exists. Both
namespaces therefore stay bound, and neither can be skipped.

Kept in its own module with no imports beyond the standard library and `strict_json`, so
the staging tool and the role entrypoint can share it without the role dragging in the
staging tool.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

#: The generation-relative path of the document. Root of the generation, beside
#: `full-manifest.json`, because it describes the generation rather than one service.
GENERATION_LEGACY_BINDING_NAME = "legacy-binding.json"

LEGACY_BINDING_SCHEMA_ID = "rquant-legacy-generation-binding/v1"
LEGACY_BINDING_SCHEMA_VERSION = 1

#: Route A stages from a legacy `data/runtime` generation; Route B stages from the
#: checkout's frozen constants and has no legacy generation to be bound to.
LEGACY_BINDING_MODE_LEGACY = "legacy"
LEGACY_BINDING_MODE_BOOTSTRAP = "bootstrap"
_MODES = (LEGACY_BINDING_MODE_BOOTSTRAP, LEGACY_BINDING_MODE_LEGACY)

#: A few hundred bytes in practice; the bound exists so a role never reads an unbounded
#: file out of a tree it is about to trust.
MAX_LEGACY_BINDING_BYTES = 4096

_FIELDS = frozenset({"schema_id", "schema_version", "mode", "runtime_root", "generation_id"})
_GENERATION_ID = re.compile(r"[0-9a-f]{64}")


class LegacyGenerationBindingError(ValueError):
    """The document is missing, malformed, or does not describe this deployment."""


@dataclass(frozen=True)
class LegacyGenerationBinding:
    """What the staging tool recorded about the legacy chain this generation came from."""

    mode: str
    runtime_root: str | None
    generation_id: str | None

    @property
    def is_legacy(self) -> bool:
        return self.mode == LEGACY_BINDING_MODE_LEGACY


def legacy_generation_binding_bytes(
    *,
    mode: str,
    runtime_root: str | None,
    generation_id: str | None,
) -> bytes:
    """The canonical document, validated on the way out as strictly as on the way in."""

    document = {
        "schema_id": LEGACY_BINDING_SCHEMA_ID,
        "schema_version": LEGACY_BINDING_SCHEMA_VERSION,
        "mode": mode,
        "runtime_root": runtime_root,
        "generation_id": generation_id,
    }
    payload = canonical_json_bytes(document, trailing_newline=True)
    parse_legacy_generation_binding(payload)
    return payload


def parse_legacy_generation_binding(payload: bytes) -> LegacyGenerationBinding:
    """Decode the document, refusing anything a staging run could not have written."""

    if len(payload) > MAX_LEGACY_BINDING_BYTES:
        raise LegacyGenerationBindingError("legacy generation binding exceeds its size bound")
    try:
        document = strict_json_loads(payload)
    except StrictJsonError as exc:
        raise LegacyGenerationBindingError(
            f"legacy generation binding is not strict JSON: {exc}"
        ) from exc
    if type(document) is not dict or set(document) != _FIELDS:
        raise LegacyGenerationBindingError("legacy generation binding schema is invalid")
    if canonical_json_bytes(document, trailing_newline=True) != payload:
        raise LegacyGenerationBindingError("legacy generation binding is not canonical")
    if document["schema_id"] != LEGACY_BINDING_SCHEMA_ID:
        raise LegacyGenerationBindingError("legacy generation binding schema id is unknown")
    if document["schema_version"] != LEGACY_BINDING_SCHEMA_VERSION:
        raise LegacyGenerationBindingError("legacy generation binding schema version is unknown")
    mode = document["mode"]
    if type(mode) is not str or mode not in _MODES:
        raise LegacyGenerationBindingError("legacy generation binding mode is unknown")
    runtime_root = document["runtime_root"]
    generation_id = document["generation_id"]
    if mode == LEGACY_BINDING_MODE_BOOTSTRAP:
        if runtime_root is not None or generation_id is not None:
            raise LegacyGenerationBindingError(
                "a bootstrap generation cannot name a legacy deployment"
            )
        return LegacyGenerationBinding(mode=mode, runtime_root=None, generation_id=None)
    if type(runtime_root) is not str or not runtime_root.startswith("/"):
        raise LegacyGenerationBindingError(
            "legacy generation binding runtime root is not one absolute path"
        )
    if ".." in runtime_root.split("/"):
        raise LegacyGenerationBindingError(
            "legacy generation binding runtime root is not canonical"
        )
    if type(generation_id) is not str or _GENERATION_ID.fullmatch(generation_id) is None:
        raise LegacyGenerationBindingError(
            "legacy generation binding generation id is not a 64-hex deployment hash"
        )
    return LegacyGenerationBinding(
        mode=mode, runtime_root=runtime_root, generation_id=generation_id
    )


__all__ = [
    "GENERATION_LEGACY_BINDING_NAME",
    "LEGACY_BINDING_MODE_BOOTSTRAP",
    "LEGACY_BINDING_MODE_LEGACY",
    "LEGACY_BINDING_SCHEMA_ID",
    "LEGACY_BINDING_SCHEMA_VERSION",
    "MAX_LEGACY_BINDING_BYTES",
    "LegacyGenerationBinding",
    "LegacyGenerationBindingError",
    "legacy_generation_binding_bytes",
    "parse_legacy_generation_binding",
]
