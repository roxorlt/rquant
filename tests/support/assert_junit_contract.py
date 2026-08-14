"""Fail closed when a JUnit report differs from its exact CI contract."""

from __future__ import annotations

import argparse
from pathlib import Path
from xml.etree import ElementTree


def _required_nonnegative_attribute(
    suite: ElementTree.Element,
    name: str,
) -> int:
    raw = suite.attrib.get(name)
    if raw is None:
        raise ValueError(f"JUnit suite is missing {name}")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"JUnit suite has invalid {name}") from exc
    if value < 0:
        raise ValueError(f"JUnit suite has negative {name}")
    return value


def _single_suite(path: Path) -> ElementTree.Element:
    root = ElementTree.parse(path).getroot()
    if root.tag == "testsuite":
        return root
    if root.tag != "testsuites":
        raise ValueError(f"JUnit root is not a suite collection: {root.tag}")
    suites = [child for child in root if child.tag == "testsuite"]
    if len(suites) != 1 or len(root) != 1:
        raise ValueError("JUnit report does not contain exactly one suite")
    return suites[0]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--suites", type=int, required=True)
    parser.add_argument("--tests", type=int, required=True)
    parser.add_argument("--failures", type=int, required=True)
    parser.add_argument("--errors", type=int, required=True)
    parser.add_argument("--skipped", type=int, required=True)
    parser.add_argument("--cases", type=int, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.suites != 1:
        raise ValueError("this verifier only accepts an exact one-suite contract")
    suite = _single_suite(args.path)
    expected = {
        "tests": args.tests,
        "failures": args.failures,
        "errors": args.errors,
        "skipped": args.skipped,
    }
    for name, value in expected.items():
        if _required_nonnegative_attribute(suite, name) != value:
            raise ValueError(f"JUnit suite {name} differs from the CI contract")
    cases = suite.findall("testcase")
    if len(cases) != args.cases:
        raise ValueError("JUnit testcase count differs from the CI contract")
    for case in cases:
        if case.findall("failure") or case.findall("error") or case.findall("skipped"):
            raise ValueError("JUnit testcase is not a passing exact node")


if __name__ == "__main__":
    main()
