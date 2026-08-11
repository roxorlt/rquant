"""Load the canonical strict JSON decoder without importing the rquant package."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_IMPLEMENTATION = Path(__file__).resolve().parents[1] / "src" / "rquant" / "strict_json.py"
_SPEC = importlib.util.spec_from_file_location("_rquant_strict_json_impl", _IMPLEMENTATION)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError("strict JSON implementation cannot be loaded")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

StrictJsonError = _MODULE.StrictJsonError
strict_json_loads = _MODULE.strict_json_loads
canonical_json_bytes = _MODULE.canonical_json_bytes
strict_canonical_json_loads = _MODULE.strict_canonical_json_loads
strict_model_validate_canonical_json = _MODULE.strict_model_validate_canonical_json
canonical_model_json_bytes = _MODULE.canonical_model_json_bytes
