"""Encoding and decoding of Sparkplug B metric values.

Sparkplug B carries signed integers in unsigned protobuf fields as two's
complement: Int8, Int16 and Int32 travel in the uint32 ``int_value`` field and
Int64 travels in the uint64 ``long_value`` field. The helpers here apply and
undo that mapping and check that a metric's value field matches its datatype.
"""

from dataclasses import dataclass

from google.protobuf.message import DecodeError

from .sparkplug_b_pb2 import DataType, Payload

__all__ = [
    "DataType",
    "DecodeError",
    "MetricValue",
    "MetricValueError",
    "Payload",
    "decode_payload",
    "decode_value",
    "make_metric",
    "make_payload",
]

Metric = Payload.Metric

# Datatype -> name of the oneof field that must carry the value.
_VALUE_FIELD: dict[int, str] = {
    DataType.Int8: "int_value",
    DataType.Int16: "int_value",
    DataType.Int32: "int_value",
    DataType.UInt8: "int_value",
    DataType.UInt16: "int_value",
    DataType.UInt32: "int_value",
    DataType.Int64: "long_value",
    DataType.UInt64: "long_value",
    DataType.DateTime: "long_value",
    DataType.Float: "float_value",
    DataType.Double: "double_value",
    DataType.Boolean: "boolean_value",
    DataType.String: "string_value",
    DataType.Text: "string_value",
    DataType.UUID: "string_value",
    DataType.Bytes: "bytes_value",
}

# Bit width of each signed integer type, used to undo two's complement.
_SIGNED_BITS: dict[int, int] = {
    DataType.Int8: 8,
    DataType.Int16: 16,
    DataType.Int32: 32,
    DataType.Int64: 64,
}

_UNSIGNED_BITS: dict[int, int] = {
    DataType.UInt8: 8,
    DataType.UInt16: 16,
    DataType.UInt32: 32,
    DataType.UInt64: 64,
}

STORABLE_TYPES = frozenset(_VALUE_FIELD)


class MetricValueError(ValueError):
    """Raised when a metric value does not match its datatype.

    ``reason`` is a short machine-readable code used as the dead-letter reason.
    """

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class MetricValue:
    """A decoded value: numeric types and booleans fill ``number``, text fills ``text``."""

    number: float | None
    text: str | None
    is_null: bool


def decode_payload(data: bytes) -> Payload:
    """Parse protobuf bytes into a Payload. Raises DecodeError on malformed input."""
    return Payload.FromString(data)


def _to_signed(raw: int, bits: int) -> int:
    raw &= (1 << bits) - 1
    return raw - (1 << bits) if raw >= 1 << (bits - 1) else raw


def decode_value(metric: Metric, datatype: int) -> MetricValue:
    """Return the value of ``metric`` interpreted as ``datatype``.

    Raises MetricValueError with reason ``unsupported_type`` for datatypes this
    pipeline does not store, ``type_mismatch`` when the populated oneof field
    does not match the datatype, ``missing_value`` when no value is set and the
    metric is not flagged null, and ``out_of_range`` when an integer does not
    fit its declared width.
    """
    if metric.is_null:
        return MetricValue(None, None, True)
    field = _VALUE_FIELD.get(datatype)
    if field is None:
        raise MetricValueError("unsupported_type", f"datatype {datatype} is not stored")
    present = metric.WhichOneof("value")
    if present is None:
        raise MetricValueError("missing_value", "metric has no value and is not flagged null")
    if present != field:
        raise MetricValueError(
            "type_mismatch",
            f"datatype {DataType.Name(datatype)} expects {field}, payload has {present}",
        )
    raw = getattr(metric, field)
    if datatype in _SIGNED_BITS:
        # Writers differ on whether Int8 -1 travels as 0xFF or 0xFFFFFFFF.
        # Values below 2**bits are read at the declared width, larger values
        # at the full field width; both encodings decode to the same number.
        bits = _SIGNED_BITS[datatype]
        wire_bits = 64 if field == "long_value" else 32
        value = _to_signed(raw, bits if raw < 1 << bits else wire_bits)
        if not -(1 << (bits - 1)) <= value < 1 << (bits - 1):
            raise MetricValueError(
                "out_of_range", f"{value} does not fit {DataType.Name(datatype)}"
            )
        return MetricValue(float(value), None, False)
    if datatype in _UNSIGNED_BITS:
        if raw >= 1 << _UNSIGNED_BITS[datatype]:
            raise MetricValueError("out_of_range", f"{raw} does not fit {DataType.Name(datatype)}")
        return MetricValue(float(raw), None, False)
    if datatype == DataType.DateTime:
        return MetricValue(float(raw), None, False)
    if datatype in (DataType.Float, DataType.Double):
        return MetricValue(float(raw), None, False)
    if datatype == DataType.Boolean:
        return MetricValue(1.0 if raw else 0.0, None, False)
    if datatype == DataType.Bytes:
        return MetricValue(None, raw.hex(), False)
    return MetricValue(None, raw, False)


def _set_value(metric: Metric, datatype: int, value: object) -> None:
    field = _VALUE_FIELD.get(datatype)
    if field is None:
        raise ValueError(f"cannot encode datatype {datatype}")
    if datatype in _SIGNED_BITS:
        wire_bits = 64 if field == "long_value" else 32
        value = int(value) & ((1 << wire_bits) - 1)
    setattr(metric, field, value)


def make_metric(
    *,
    datatype: int,
    value: object = None,
    name: str | None = None,
    alias: int | None = None,
    timestamp: int | None = None,
    is_historical: bool = False,
    send_datatype: bool = True,
) -> Metric:
    """Build a Metric. A ``value`` of None produces a metric flagged null."""
    metric = Metric()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    if timestamp is not None:
        metric.timestamp = timestamp
    if send_datatype:
        metric.datatype = datatype
    if is_historical:
        metric.is_historical = True
    if value is None:
        metric.is_null = True
    else:
        _set_value(metric, datatype, value)
    return metric


def make_payload(
    *, timestamp: int | None, seq: int | None, metrics: list[Metric] | tuple[Metric, ...] = ()
) -> Payload:
    payload = Payload()
    if timestamp is not None:
        payload.timestamp = timestamp
    if seq is not None:
        payload.seq = seq
    payload.metrics.extend(metrics)
    return payload
