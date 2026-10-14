import pytest

from sparkplug_pipeline.sparkplug.payload import (
    DataType,
    DecodeError,
    MetricValueError,
    decode_payload,
    decode_value,
    make_metric,
    make_payload,
)
from sparkplug_pipeline.sparkplug.sparkplug_b_pb2 import Payload


def roundtrip(metric):
    data = make_payload(timestamp=1, seq=0, metrics=[metric]).SerializeToString()
    return decode_payload(data).metrics[0]


def test_payload_roundtrip_keeps_header_and_metrics():
    payload = make_payload(
        timestamp=1_767_225_600_000,
        seq=7,
        metrics=[make_metric(name="temperature", alias=3, datatype=DataType.Float, value=21.5)],
    )
    decoded = decode_payload(payload.SerializeToString())
    assert decoded.timestamp == 1_767_225_600_000
    assert decoded.seq == 7
    assert decoded.metrics[0].name == "temperature"
    assert decoded.metrics[0].alias == 3


def test_wire_format_uses_spec_field_numbers():
    # Payload.seq is field 3 (varint): tag byte 0x18. Metric.alias is field 2: tag 0x10.
    payload = make_payload(timestamp=None, seq=5, metrics=[])
    assert payload.SerializeToString() == b"\x18\x05"
    metric = make_metric(alias=9, datatype=DataType.Boolean, value=True, send_datatype=False)
    # alias (0x10 0x09) then boolean_value field 14 varint (0x70 0x01)
    assert metric.SerializeToString() == b"\x10\x09\x70\x01"


@pytest.mark.parametrize(
    ("datatype", "value"),
    [
        (DataType.Int8, -128),
        (DataType.Int8, 127),
        (DataType.Int16, -2),
        (DataType.Int32, -2_000_000_000),
        (DataType.Int64, -9_000_000_000_000),
        (DataType.UInt8, 255),
        (DataType.UInt32, 4_000_000_000),
        (DataType.UInt64, 2**63),
        (DataType.Double, 3.25),
        (DataType.Boolean, True),
    ],
)
def test_numeric_values_roundtrip(datatype, value):
    decoded = decode_value(roundtrip(make_metric(datatype=datatype, value=value)), datatype)
    assert decoded.number == float(value)
    assert decoded.text is None
    assert not decoded.is_null


def test_signed_int8_accepts_both_wire_encodings():
    short = make_metric(datatype=DataType.Int8, value=0)
    short.int_value = 0xFF  # -1 written at 8-bit width
    wide = make_metric(datatype=DataType.Int8, value=-1)  # -1 written at 32-bit width
    assert wide.int_value == 0xFFFFFFFF
    assert decode_value(short, DataType.Int8).number == -1
    assert decode_value(wide, DataType.Int8).number == -1


def test_float_is_read_at_single_precision():
    decoded = decode_value(
        roundtrip(make_metric(datatype=DataType.Float, value=0.1)), DataType.Float
    )
    assert decoded.number != 0.1
    assert decoded.number == pytest.approx(0.1, rel=1e-7)


def test_string_and_null_values():
    text = decode_value(make_metric(datatype=DataType.String, value="run"), DataType.String)
    assert (text.number, text.text) == (None, "run")
    null = decode_value(make_metric(datatype=DataType.Double, value=None), DataType.Double)
    assert null.is_null and null.number is None


@pytest.mark.parametrize(
    ("metric", "datatype", "reason"),
    [
        (make_metric(datatype=DataType.String, value="hot"), DataType.Float, "type_mismatch"),
        (Payload.Metric(alias=1), DataType.Double, "missing_value"),
        (make_metric(datatype=DataType.UInt32, value=300), DataType.UInt8, "out_of_range"),
        (Payload.Metric(alias=1), DataType.Template, "unsupported_type"),
    ],
)
def test_invalid_values_raise_with_reason(metric, datatype, reason):
    with pytest.raises(MetricValueError) as info:
        decode_value(metric, datatype)
    assert info.value.reason == reason


def test_garbage_bytes_fail_to_decode():
    with pytest.raises(DecodeError):
        decode_payload(b"not a protobuf")
