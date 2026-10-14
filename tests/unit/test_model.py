"""Batch dedupe and rollup bucket placement."""

from dataclasses import replace

from sparkplug_pipeline.ingest.model import Sample, dedupe_batch, minute_bucket, touched_buckets

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def sample(event_ms, value=1.0, metric="temperature", device="press-01", **kw):
    base = Sample(
        event_ms=event_ms,
        group_id="plant1",
        edge_node="edge-01",
        device=device,
        metric=metric,
        datatype=9,
        value=value,
        value_text=None,
        is_null=False,
        is_historical=False,
        is_late=False,
        ingest_ms=T0,
        bd_seq=0,
        seq=0,
        schema_version=1,
    )
    return replace(base, **kw)


def test_minute_bucket_floors_to_minute():
    assert minute_bucket(T0) == T0
    assert minute_bucket(T0 + 59_999) == T0
    assert minute_bucket(T0 + 60_000) == T0 + 60_000


def test_dedupe_keeps_first_sample_per_key():
    first = sample(T0, value=1.0)
    repeat = sample(T0, value=2.0, seq=9)
    other_metric = sample(T0, metric="vibration")
    kept, dropped = dedupe_batch([first, repeat, other_metric, sample(T0 + 1)])
    assert dropped == 1
    assert kept[0] is first
    assert other_metric in kept


def test_dedupe_is_idempotent():
    batch = [sample(T0 + i * 1000) for i in range(5)] * 2
    once, dropped_once = dedupe_batch(batch)
    twice, dropped_twice = dedupe_batch(once)
    assert (dropped_once, dropped_twice) == (5, 0)
    assert once == twice


def test_touched_buckets_use_event_time():
    keys = [
        sample(T0 + 125_000).key,  # minute 2
        sample(T0 + 130_000).key,  # minute 2 again
        sample(T0 + 5_000, metric="vibration").key,
        sample(T0 + 5_000, device="press-02").key,
    ]
    assert touched_buckets(keys) == [
        ("plant1", "edge-01", "press-01", "temperature", T0 + 120_000),
        ("plant1", "edge-01", "press-01", "vibration", T0),
        ("plant1", "edge-01", "press-02", "temperature", T0),
    ]
