"""Independent exact rational reconstruction of the already frozen sampler."""

from __future__ import annotations

from fractions import Fraction
import hashlib
import json
from pathlib import Path


def reference(observations: tuple[str, ...], days: int) -> list[dict[str, str]]:
    position = 20261005
    mask = 18446744073709551615
    values = [Fraction(1) for _ in range(2048)]
    choices = [Fraction(value) for value in observations]
    result = []
    for _ in range(days):
        for member in range(2048):
            while True:
                position = (position + 11400714819323198485) & mask
                mixed = position
                mixed = ((mixed ^ (mixed >> 30)) * 13787848793156543929) & mask
                mixed = ((mixed ^ (mixed >> 27)) * 10723151780598845931) & mask
                sample = mixed ^ (mixed >> 31)
                if sample < ((1 << 64) // len(choices)) * len(choices):
                    break
            values[member] *= 1 + choices[sample % len(choices)]
        ordered = sorted(values)
        result.append({"lower_fraction": str(ordered[102]), "upper_fraction": str(ordered[1945])})
    return result


if __name__ == "__main__":
    target = Path(__file__).with_name("bootstrap-independent-reference.json")
    payload = {"source": "Independent Fraction calculation; imports no rquant/product modules", "frozen_seed": 20261005,
               "paths": 2048, "returns": ["-.01", "0", ".02"], "days": reference(("-.01", "0", ".02"), 8),
               "zero_returns": reference(("0",), 3)}
    target.write_text(json.dumps(payload, indent=2) + "\n")
    print("reference_sha256=" + hashlib.sha256(target.read_bytes()).hexdigest())
