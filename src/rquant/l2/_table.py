"""Direct port of reviewed lossless table decode; no exporter or CLI."""
from __future__ import annotations
from decimal import Decimal, localcontext
import hashlib
import struct
from typing import Any

RLE_REFERENCE = "https://github.com/wukan1986/10jqka/blob/42161ded5b88e1542c6d6ddd056a84a9dcfe22a0/ftj.py"

INTERPRETATION_LIMITS = (
    "字段ID、flags、原始字节和style按供应商格式保留；未确认单位、买卖枚举或交易所channel。"
    "标题为明细的表仅标记为明细，尚不能据此认定是逐笔成交。"
)

def decode_packed32(raw: bytes) -> str | None:
    """Decode the observed little-endian packed value without float rounding."""
    if len(raw) != 4:
        raise ValueError("packed32 requires exactly four bytes")
    value = int.from_bytes(raw, "little")
    if value in (0xFFFFFFFF, 0x80000000):
        return None
    exponent = (value >> 28) & 7
    if value & 0x80000000:
        exponent = -exponent
    with localcontext() as context:
        context.prec = 32
        number = Decimal(value & 0x07FFFFFF).scaleb(exponent)
        if value & 0x08000000:
            number = -number
        return format(number, "f")

def _decode_rle(source: bytes, expected: int) -> bytes:
    """Decode ftj's control tree with strict input/output bounds.

    The reference's unrolled control tree is expressed as repeat lengths here.
    Control bits are MSB first. Loading the next control byte is eager: the
    eighth bit consumes that byte before any following literal/run operand.
    """
    if expected == 0:
        if source:
            raise ValueError("nonempty compressed data for an empty table")
        return b""
    if len(source) < 2:
        raise ValueError("truncated RLE initial control/literal")
    position, control, remaining = 2, source[0], 8
    last = source[1]
    output = bytearray([last])

    def take() -> int:
        nonlocal position
        if position >= len(source):
            raise ValueError("truncated RLE input")
        value = source[position]
        position += 1
        return value

    def bit() -> int:
        nonlocal control, remaining
        value = (control >> 7) & 1
        control = (control << 1) & 255
        remaining -= 1
        if remaining == 0:
            control, remaining = take(), 8
        return value

    while len(output) < expected:
        if not bit():
            literals = bytes((take(), take()))
            if len(output) + 2 > expected:
                raise ValueError("RLE literal exceeds declared decoded size")
            output.extend(literals)
            last = literals[-1]
            continue
        if not bit():
            last = take()
            output.append(last)
        if not bit():
            count = 1
        elif not bit():
            count = 2 + bit()
        elif not bit():
            count = 4
        elif not bit():
            count = 5 + 2 * bit() + bit()
        elif not bit():
            count = 9 + bit()
        elif not bit():
            count = 11
        else:
            count = 12
            while True:
                extra = take()
                if extra > 127:
                    extra = ((extra - 128) << 8) + take()
                count += extra
                if len(output) + count > expected:
                    raise ValueError("RLE repeat exceeds declared decoded size")
                if extra != 32767:
                    break
        if len(output) + count > expected:
            raise ValueError("RLE repeat exceeds declared decoded size")
        output.extend(bytes((last,)) * count)
    if len(output) != expected or position != len(source):
        raise ValueError("RLE size mismatch or trailing input")
    return bytes(output)

def _match_int(metadata: dict[str, Any], name: str, actual: int) -> None:
    if type(metadata.get(name)) is not int or metadata[name] != actual:
        raise ValueError(f"metadata {name} mismatch")

def parse_table(data: bytes, metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate one decompressed table and retain all row bytes and styles."""
    source_sha256 = hashlib.sha256(data).hexdigest()
    if metadata.get("sha256") != source_sha256:
        raise ValueError("table sha256 mismatch")
    _match_int(metadata, "decoded_bytes", len(data))
    if len(data) < 24:
        raise ValueError("truncated table header")
    version = data[:6].rstrip(b"\0").decode("ascii")
    if version not in ("tb1.0", "tb3.0") or version != metadata.get("table_version"):
        raise ValueError("unsupported or mismatched table version")
    rows, unknown10, unknown14, ext, stride, count = struct.unpack_from("<iiiHHH", data, 6)
    if rows < 0 or count == 0 or stride == 0 or ext < 24 + count * 4 or ext > len(data):
        raise ValueError("invalid table dimensions or extended header")
    for name, actual in [
        ("row_count", rows), ("row_width", stride), ("col_count", count),
        ("extended_header_length", ext), ("data_bytes_after_header", len(data) - ext),
    ]:
        _match_int(metadata, name, actual)
    descriptions = metadata.get("columns")
    if not isinstance(descriptions, list) or len(descriptions) != count:
        raise ValueError("column metadata count mismatch")
    columns: list[dict[str, Any]] = []
    encodings = {0: "utf-16le", 0x1000: "packed32-le", 0x2000: "unsigned-le"}
    for index in range(count):
        flags, width, extra = struct.unpack_from("<HBB", data, 24 + index * 4)
        field_id, data_type = flags & 0x8FFF, flags & 0x7000
        description = descriptions[index]
        if not isinstance(description, dict) or width == 0:
            raise ValueError("invalid column metadata or width")
        for name, actual in [("field_id", field_id), ("width", width), ("extra", extra)]:
            _match_int(description, name, actual)
        if int(description.get("flags_hex", ""), 16) != flags:
            raise ValueError("column flags mismatch")
        if data_type == 0 and width % 2 or data_type == 0x1000 and width != 4:
            raise ValueError("invalid column width for its data type")
        columns.append({"field_id": field_id, "flags": flags, "flags_hex": hex(flags), "width": width, "extra": extra, "encoding": encodings.get(data_type, "raw-hex")})
    if sum(column["width"] + 1 for column in columns) != stride:
        raise ValueError("column widths plus style bytes do not equal row_width")
    if len({column["field_id"] for column in columns}) != count:
        raise ValueError("duplicate field IDs cannot be represented losslessly")
    expected = rows * stride
    if version == "tb1.0":
        row_data = data[ext:]
    else:
        if len(data) - ext < 8:
            raise ValueError("truncated tb3 compression header")
        compressed_size = int.from_bytes(data[ext : ext + 4], "little")
        decoded_size = int.from_bytes(data[ext + 4 : ext + 8], "big")
        if compressed_size != len(data) - ext - 4 or decoded_size != expected:
            raise ValueError("tb3 compressed/decoded size mismatch")
        decoded = _decode_rle(data[ext + 8 :], expected)
        row_data = bytes(decoded[column * rows + row] for row in range(rows) for column in range(stride))
    if len(row_data) != expected:
        raise ValueError("row_count times row_width does not match decoded row bytes")
    values: list[dict[str, Any]] = []
    for row_index in range(rows):
        position = row_index * stride
        cells: dict[str, Any] = {}
        for column in columns:
            width = column["width"]
            raw = row_data[position : position + width]
            encoding = column["encoding"]
            if encoding == "utf-16le":
                value: str | int | None = raw.decode("utf-16le").rstrip("\0")
            elif encoding == "packed32-le":
                value = decode_packed32(raw)
            elif encoding == "unsigned-le":
                value = int.from_bytes(raw, "little")
            else:
                value = "0x" + raw.hex()
            cells[str(column["field_id"])] = {"value": value, "raw_hex": raw.hex(), "style": row_data[position + width]}
            position += width + 1
        values.append(cells)
    parsed = {
        "selected_frame": metadata["selected_frame"],
        "selected_frames": metadata.get("selected_frames", [metadata["selected_frame"]]),
        "title": metadata["title"],
        "table_version": version,
        "source_codec": metadata["codec"],
        "source_sha256": source_sha256,
        "row_data_sha256": hashlib.sha256(row_data).hexdigest(),
        "row_count": rows,
        "row_width": stride,
        "col_count": count,
        "extended_header_length": ext,
        "header_unknown_10": unknown10,
        "header_unknown_14": unknown14,
        "columns": columns,
        "rows": values,
        "interpretation_limits": INTERPRETATION_LIMITS,
    }
    if "record_file" in metadata:
        parsed["record_file"] = metadata["record_file"]
    if version == "tb3.0":
        parsed["rle_algorithm_reference"] = RLE_REFERENCE
    return parsed
