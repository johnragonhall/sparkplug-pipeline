"""Processor behavior: alias resolution, sessions, validation, lateness and schemas."""

import pytest

from sparkplug_pipeline.ingest.model import minute_bucket
from sparkplug_pipeline.ingest.processor import Processor, ProcessorConfig
from sparkplug_pipeline.sparkplug.payload import DataType, make_metric, make_payload

G, N = "plant1", "edge-01"
T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
NOW = T0 + 3_600_000  # receive time used by these tests


def topic(kind, device=None):
    base = f"spBv1.0/{G}/{kind}/{N}"
    return base if device is None else f"{base}/{device}"


def nbirth(bd_seq=0, ts=T0):
    return make_payload(
        timestamp=ts,
        seq=0,
        metrics=[
            make_metric(name="bdSeq", datatype=DataType.Int64, value=bd_seq),
            make_metric(name="Node Control/Rebirth", datatype=DataType.Boolean, value=False),
            make_metric(name="uptime_s", alias=1, datatype=DataType.UInt32, value=5, timestamp=ts),
        ],
    ).SerializeToString()


def ndeath(bd_seq=0):
    return make_payload(
        timestamp=None,
        seq=None,
        metrics=[make_metric(name="bdSeq", datatype=DataType.Int64, value=bd_seq)],
    ).SerializeToString()


DEFAULT_METRICS = (("temperature", 10, DataType.Float), ("vibration", 11, DataType.Double))


def dbirth(seq, ts=T0, metrics=DEFAULT_METRICS):
    return make_payload(
        timestamp=ts,
        seq=seq,
        metrics=[
            make_metric(name=name, alias=alias, datatype=dt, value=1, timestamp=ts)
            for name, alias, dt in metrics
        ],
    ).SerializeToString()


def ddata(seq, *metrics, ts=T0):
    return make_payload(timestamp=ts, seq=seq, metrics=list(metrics)).SerializeToString()


def alias_metric(alias, value, ts, datatype=DataType.Float, **kw):
    return make_metric(
        alias=alias, datatype=datatype, value=value, timestamp=ts, send_datatype=False, **kw
    )


@pytest.fixture
def proc():
    p = Processor(ProcessorConfig(allowed_lateness_ms=10_000))
    p.handle(topic("NBIRTH"), nbirth(), NOW)
    p.handle(topic("DBIRTH", "press-01"), dbirth(1), NOW)
    return p


def test_alias_only_data_resolves_names_from_birth(proc):
    out = proc.handle(
        topic("DDATA", "press-01"),
        ddata(
            2,
            alias_metric(10, 41.5, T0 + 6000),
            alias_metric(11, 0.25, T0 + 6000, datatype=DataType.Double),
        ),
        NOW,
    )
    assert out.dead_letter is None
    assert [(s.metric, s.value) for s in out.samples] == [
        ("temperature", 41.5),
        ("vibration", 0.25),
    ]
    assert all(s.device == "press-01" and s.bd_seq == 0 and s.seq == 2 for s in out.samples)


def test_birth_metrics_become_samples_but_control_metrics_do_not():
    p = Processor()
    out = p.handle(topic("NBIRTH"), nbirth(), NOW)
    assert [s.metric for s in out.samples] == ["uptime_s"]
    assert [e.event for e in out.events] == ["nbirth"]


def test_aliases_are_scoped_to_the_session(proc):
    # A new session assigns alias 10 to a different metric; old meanings must not leak.
    proc.handle(topic("NBIRTH"), nbirth(bd_seq=1), NOW)
    swapped = (("vibration", 10, DataType.Double), ("temperature", 11, DataType.Float))
    proc.handle(topic("DBIRTH", "press-01"), dbirth(1, metrics=swapped), NOW)
    out = proc.handle(
        topic("DDATA", "press-01"),
        ddata(2, alias_metric(10, 0.5, T0 + 6000, datatype=DataType.Double)),
        NOW,
    )
    assert [s.metric for s in out.samples] == ["vibration"]


def test_unknown_alias_dead_letters_the_whole_message(proc):
    out = proc.handle(
        topic("DDATA", "press-01"),
        ddata(2, alias_metric(10, 40.0, T0 + 6000), alias_metric(999, 1.0, T0 + 6000)),
        NOW,
    )
    assert out.samples == []
    assert out.dead_letter.reason == "unknown_alias"


def test_alias_of_another_device_is_rejected(proc):
    proc.handle(topic("DBIRTH", "press-02"), dbirth(2, metrics=(("temperature", 20, 9),)), NOW)
    out = proc.handle(topic("DDATA", "press-01"), ddata(3, alias_metric(20, 1.0, T0)), NOW)
    assert out.dead_letter.reason == "unknown_alias"


@pytest.mark.parametrize(
    ("kind", "device", "payload", "reason"),
    [
        ("DDATA", "press-01", b"not a protobuf", "decode_error"),
        ("DDATA", "ghost-01", ddata(2, alias_metric(10, 1.0, T0)), "device_not_born"),
        (
            "DDATA",
            "press-01",
            ddata(
                2,
                make_metric(
                    alias=10,
                    datatype=DataType.String,
                    value="hot",
                    timestamp=T0,
                    send_datatype=False,
                ),
            ),
            "type_mismatch",
        ),
        ("DDATA", "press-01", ddata(2, alias_metric(10, 1.0, None), ts=None), "missing_timestamp"),
        ("DDATA", "press-01", ddata(2, alias_metric(10, 1.0, NOW + 3_600_000)), "future_timestamp"),
        (
            "DDATA",
            "press-01",
            make_payload(timestamp=T0, seq=None).SerializeToString(),
            "missing_seq",
        ),
    ],
)
def test_invalid_messages_are_dead_lettered(proc, kind, device, payload, reason):
    out = proc.handle(topic(kind, device), payload, NOW)
    assert out.dead_letter is not None
    assert out.dead_letter.reason == reason
    assert out.samples == []


def test_bad_topic_is_dead_lettered():
    out = Processor().handle(f"spBv1.0/{G}/DDATA/{N}", b"", NOW)
    assert out.dead_letter.reason == "bad_topic"


def test_data_before_nbirth_has_no_session():
    out = Processor().handle(topic("DDATA", "press-01"), ddata(1, alias_metric(10, 1.0, T0)), NOW)
    assert out.dead_letter.reason == "no_session"


def test_ddeath_takes_device_offline(proc):
    proc.handle(topic("DDEATH", "press-01"), ddata(2), NOW)
    out = proc.handle(topic("DDATA", "press-01"), ddata(3, alias_metric(10, 1.0, T0)), NOW)
    assert out.dead_letter.reason == "device_offline"


def test_ndeath_ends_the_session_and_stale_ndeath_is_ignored(proc):
    stale = proc.handle(topic("NDEATH"), ndeath(bd_seq=7), NOW)
    assert [e.event for e in stale.events] == ["stale_ndeath"]
    out = proc.handle(topic("NDEATH"), ndeath(bd_seq=0), NOW)
    assert [e.event for e in out.events] == ["ndeath"]
    after = proc.handle(topic("DDATA", "press-01"), ddata(2, alias_metric(10, 1.0, T0)), NOW)
    assert after.dead_letter.reason == "no_session"


def test_rebirth_with_same_bdseq_is_reported_as_rebirth(proc):
    out = proc.handle(topic("NBIRTH"), nbirth(bd_seq=0), NOW)
    assert [e.event for e in out.events] == ["rebirth"]
    # The rebirth clears device state: data needs a fresh DBIRTH.
    data = proc.handle(topic("DDATA", "press-01"), ddata(1, alias_metric(10, 1.0, T0)), NOW)
    assert data.dead_letter.reason == "device_not_born"


def test_seq_anomalies_become_events_and_data_is_kept(proc):
    out = proc.handle(topic("DDATA", "press-01"), ddata(4, alias_metric(10, 1.0, T0 + 1)), NOW)
    assert [(e.event, e.seq) for e in out.events] == [("seq_gap", 4)]
    assert len(out.samples) == 1
    late = proc.handle(topic("DDATA", "press-01"), ddata(2, alias_metric(10, 1.0, T0 + 2)), NOW)
    assert [e.event for e in late.events] == ["out_of_order"]
    dup = proc.handle(topic("DDATA", "press-01"), ddata(4, alias_metric(10, 1.0, T0 + 1)), NOW)
    assert [e.event for e in dup.events] == ["duplicate_seq"]
    assert len(dup.samples) == 1  # dropped later by the dedupe key


def test_lateness_uses_event_time_watermark(proc):
    def send(seq, ts, historical=False):
        m = alias_metric(10, 1.0, ts, is_historical=historical)
        return proc.handle(topic("DDATA", "press-01"), ddata(seq, m), NOW).samples[0]

    assert not send(2, T0 + 120_000).is_late
    assert not send(3, T0 + 112_000).is_late  # 8 s behind: inside the 10 s allowance
    replayed = send(4, T0 + 30_000, historical=True)
    assert replayed.is_late and replayed.is_historical
    # The watermark never moves backwards.
    assert send(5, T0 + 60_000).is_late


def test_late_sample_lands_in_bucket_of_its_event_time(proc):
    proc.handle(topic("DDATA", "press-01"), ddata(2, alias_metric(10, 1.0, T0 + 600_000)), NOW)
    late = proc.handle(
        topic("DDATA", "press-01"), ddata(3, alias_metric(10, 1.0, T0 + 95_000)), NOW
    ).samples[0]
    assert late.is_late
    assert minute_bucket(late.event_ms) == T0 + 60_000
    assert late.ingest_ms == NOW


def test_schema_versions_follow_metric_set_changes():
    p = Processor()
    p.handle(topic("NBIRTH"), nbirth(), NOW)
    first = p.handle(topic("DBIRTH", "press-01"), dbirth(1), NOW)
    assert first.schema.version == 1
    p.handle(topic("NBIRTH"), nbirth(bd_seq=1), NOW)
    same = p.handle(topic("DBIRTH", "press-01"), dbirth(1), NOW)
    assert same.schema is None
    assert same.samples[0].schema_version == 1
    grown = DEFAULT_METRICS + (("motor_current", 12, DataType.Double),)
    changed = p.handle(topic("DBIRTH", "press-01"), dbirth(2, metrics=grown), NOW)
    assert changed.schema.version == 2
    assert ("motor_current", DataType.Double) in changed.schema.metrics
    assert {s.schema_version for s in changed.samples} == {2}


def test_schema_numbering_continues_from_loaded_state():
    sig = (("temperature", DataType.Float),)
    p = Processor(schemas={(G, N, "press-01"): (3, sig)})
    p.handle(topic("NBIRTH"), nbirth(), NOW)
    out = p.handle(topic("DBIRTH", "press-01"), dbirth(1), NOW)
    assert out.schema.version == 4


def test_invalid_dbirth_leaves_device_unborn():
    p = Processor()
    p.handle(topic("NBIRTH"), nbirth(), NOW)
    bad = make_payload(
        timestamp=T0,
        seq=1,
        metrics=[
            make_metric(name="temperature", alias=10, datatype=DataType.Float, value=None),
            make_metric(name="mode", alias=11, datatype=DataType.Template, value=None),
        ],
    )
    bad.metrics[1].is_null = False  # a Template value this pipeline cannot store
    out = p.handle(topic("DBIRTH", "press-01"), bad.SerializeToString(), NOW)
    assert out.dead_letter.reason == "unsupported_type"
    assert out.schema is None
    assert (G, N, "press-01") not in p.schemas
    data = p.handle(topic("DDATA", "press-01"), ddata(2, alias_metric(10, 1.0, T0)), NOW)
    assert data.dead_letter.reason == "device_not_born"


def test_state_and_command_topics_are_ignored():
    p = Processor()
    assert p.handle("spBv1.0/STATE/ingest-01", b"{}", NOW).ignored
    assert p.handle(topic("NCMD"), b"", NOW).ignored
