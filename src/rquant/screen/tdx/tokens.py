"""Bounded tokenizer; no Python or SQL syntax is accepted."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal

from rquant.screen.tdx.ast import ParseIssue, SourcePosition

MAX_SOURCE_BYTES = 4096
MAX_TOKENS = 1024
MAX_IDENTIFIER_CHARS = 32


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    position: SourcePosition


class FormulaError(Exception):
    def __init__(self, issue: ParseIssue) -> None:
        super().__init__(issue.message)
        self.issue = issue


def source_position(source: str, offset: int) -> SourcePosition:
    line = source.count("\n", 0, offset) + 1
    prior_newline = source.rfind("\n", 0, offset)
    return SourcePosition(line=line, column=offset - prior_newline, offset=offset)


def _reject(source: str, offset: int, code: str, message: str) -> None:
    raise FormulaError(
        ParseIssue(code=code, message=message, position=source_position(source, offset))
    )


def _name_start(character: str) -> bool:
    return (character.isascii() and (character.isalpha() or character == "_")) or (
        "\u3400" <= character <= "\u9fff"
    )


def _name_part(character: str) -> bool:
    return _name_start(character) or (character.isascii() and character.isdigit())


def tokenize(source: str) -> list[Token]:
    if len(source) > MAX_SOURCE_BYTES:
        _reject(source, 0, "limit", "公式太长，请删减后重试。")
    try:
        source_bytes = source.encode("utf-8")
    except UnicodeEncodeError as error:
        _reject(source, error.start, "syntax", "这里有无效文字，请修改公式。")
    if len(source_bytes) > MAX_SOURCE_BYTES:
        _reject(source, 0, "limit", "公式太长，请删减后重试。")
    tokens: list[Token] = []
    index = 0
    while index < len(source):
        character = source[index]
        if character.isspace():
            index += 1
            continue
        start = index
        if _name_start(character):
            index += 1
            while index < len(source) and _name_part(source[index]):
                index += 1
            value = source[start:index].upper()
            if len(value) > MAX_IDENTIFIER_CHARS:
                _reject(source, start, "limit", "名称太长，请缩短后重试。")
            kind = "identifier"
        elif character.isascii() and (
            character.isdigit() or (character == "." and source[index + 1 : index + 2].isdigit())
        ):
            index += 1
            while index < len(source) and source[index].isascii() and source[index].isdigit():
                index += 1
            if index < len(source) and source[index] == ".":
                index += 1
                while index < len(source) and source[index].isascii() and source[index].isdigit():
                    index += 1
            value = source[start:index]
            converted = float(value)
            if not math.isfinite(converted):
                _reject(source, start, "range", "数字过大，请改小后重试。")
            if converted == 0 and Decimal(value) != 0:
                _reject(source, start, "range", "数字过小，请改大后重试。")
            kind = "number"
        elif source[index : index + 2] in {":=", ">=", "<=", "<>", "!="}:
            value = source[index : index + 2]
            index += 2
            kind = "operator"
        elif character in "+-*/><=(),;:":
            value = character
            index += 1
            kind = "operator"
        else:
            _reject(source, start, "syntax", "这里有不支持的符号，请修改公式。")
        tokens.append(Token(kind, value, source_position(source, start)))
        if len(tokens) > MAX_TOKENS:
            _reject(source, start, "limit", "公式项太多，请删减后重试。")
    tokens.append(Token("eof", "", source_position(source, len(source))))
    return tokens
