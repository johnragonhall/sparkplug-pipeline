"""End-to-end test against the docker compose stack.

    uv run pytest -m integration

The test builds the same scenario plan the simulator runs and derives every
expectation from it: the rows that must exist, the duplicates, the dead
letters and the late rows with their buckets. It then checks the database.

Set COMPOSE to the compose command when it is not ``docker compose`` (for
example ``wsl -d Ubuntu -e docker compose`` on a Windows host whose Docker
engine runs in WSL). Set SPB_KEEP_STACK=1 to leave the stack running.
"""

import os
import shlex
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path

import pg8000.native
import pytest

from sparkplug_pipeline.ingest.model import MINUTE_MS, minute_bucket
from sparkplug_pipeline.sim.scenario import (
    DEVICES,
    OUTAGE_START_TICK,
    RECONNECT_TICK,
    TICK_MS,
    Publish,
    build_scenario,
)

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = shlex.split(os.environ.get("COMPOSE", "docker compose"))
DB_PORT = int(os.environ.get("SPB_DB_PORT", "15433"))
SCENARIO = build_scenario()


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*COMPOSE, *args], cwd=ROOT, check=check, text=True, capture_output=True, timeout=900
    )


def counters(conn) -> dict[str, int]:
    return dict(conn.run("SELECT name, value FROM ingest_counters"))


def wait_for_messages(conn, expected: int, timeout: float = 120) -> None:
    """Wait until the ingest service has committed ``expected`` scenario messages."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        c = counters(conn)
        if c.get("messages_received", 0) - c.get("messages_ignored", 0) >= expected:
            return
        time.sleep(0.25)
    raise AssertionError(f"ingest did not reach {expected} messages: {counters(conn)}")


@pytest.fixture(scope="module")
def db():
    compose("down", "-v", "--remove-orphans")
    try:
        compose("up", "-d", "--build")
        compose("wait", "sim")
        conn = pg8000.native.Connection(
            host="127.0.0.1",
            port=DB_PORT,
            database="telemetry",
            user="postgres",
            password="sparkplug-demo",
        )
        wait_for_messages(conn, SCENARIO.messages_on_the_wire)
        yield conn
        conn.close()
    except Exception:
        print(compose("logs", "--no-color", check=False).stdout[-20_000:])
        raise
    finally:
        if os.environ.get("SPB_KEEP_STACK") != "1":
            compose("down", "-v", "--remove-orphans", check=False)


# Expectations derived from the plan ------------------------------------------


def expected_rows():
    """First occurrence of each sample key among the valid messages, in publish order."""
    rows = {}
    for step in SCENARIO.publishes:
        if step.expect != "ok":
            continue
        for s in step.samples:
            rows.setdefault((s.device, s.metric, s.event_ms), s)
    return rows


EXPECTED = expected_rows()
# Every replayed reading that was not already stored arrives behind the watermark.
EXPECTED_LATE = {k for k, s in EXPECTED.items() if s.historical}
VALID_SAMPLES = sum(len(p.samples) for p in SCENARIO.publishes if p.expect == "ok")


def test_scenario_shape():
    # Guards against the plan drifting away from what the README describes.
    assert SCENARIO.messages_on_the_wire == 513
    assert len(EXPECTED) == 1819
    assert len(EXPECTED_LATE) == 270
    assert VALID_SAMPLES - len(EXPECTED) == 30


def test_every_message_was_received(db):
    c = counters(db)
    assert c["messages_received"] - c.get("messages_ignored", 0) == SCENARIO.messages_on_the_wire


def test_stored_rows_match_the_plan(db):
    rows = db.run(
        """SELECT device, metric, (extract(epoch FROM event_time) * 1000)::bigint, value
           FROM metric_samples WHERE group_id = 'plant1'"""
    )
    stored = {(d, m, ts): v for d, m, ts, v in rows}
    assert stored.keys() == EXPECTED.keys()
    for key, sample in EXPECTED.items():
        assert stored[key] == pytest.approx(sample.value, rel=1e-12), key


def test_duplicates_were_dropped(db):
    c = counters(db)
    assert c["samples_inserted"] == len(EXPECTED)
    assert c["samples_duplicate"] == VALID_SAMPLES - len(EXPECTED)


def test_dead_letters_match_the_plan(db):
    expected = Counter(p.expect for p in SCENARIO.publishes if p.expect != "ok")
    assert sum(expected.values()) == 6
    stored = Counter(dict(db.run("SELECT reason, count(*) FROM dead_letters GROUP BY reason")))
    assert stored == expected
    payloads = {(t, bytes(p)) for t, p in db.run("SELECT topic, payload FROM dead_letters")}
    assert payloads == {(p.topic, p.payload) for p in SCENARIO.publishes if p.expect != "ok"}


def test_late_rows_are_flagged_and_sit_in_their_event_time_buckets(db):
    rows = db.run(
        """SELECT device, metric, (extract(epoch FROM event_time) * 1000)::bigint,
                  is_historical, ingest_time
           FROM metric_samples WHERE is_late"""
    )
    assert {(d, m, ts) for d, m, ts, *_ in rows} == EXPECTED_LATE
    assert all(hist for *_, hist, _ in rows)
    first_late = SCENARIO.start_ms + OUTAGE_START_TICK * TICK_MS
    last_late = SCENARIO.start_ms + (RECONNECT_TICK - 1) * TICK_MS
    assert min(ts for _, _, ts, *_ in rows) == first_late
    assert max(ts for _, _, ts, *_ in rows) == last_late


def test_rollups_equal_aggregates_of_the_expected_rows(db):
    agg = defaultdict(list)
    for (device, metric, ts), sample in EXPECTED.items():
        agg[(device, metric, minute_bucket(ts))].append(
            ((device, metric, ts) in EXPECTED_LATE, sample.value)
        )
    rows = db.run(
        """SELECT device, metric, (extract(epoch FROM bucket) * 1000)::bigint,
                  sample_count, late_count, value_avg, value_min, value_max
           FROM metric_rollup_1m WHERE group_id = 'plant1'"""
    )
    stored = {(d, m, b): rest for d, m, b, *rest in rows}
    assert stored.keys() == agg.keys()
    for key, items in agg.items():
        count, late, avg, lo, hi = stored[key]
        values = [v for _, v in items]
        assert count == len(items), key
        assert late == sum(is_late for is_late, _ in items), key
        assert avg == pytest.approx(sum(values) / len(values), rel=1e-9), key
        assert (lo, hi) == (pytest.approx(min(values)), pytest.approx(max(values))), key


def test_buckets_that_received_late_rows_were_refreshed(db):
    start = SCENARIO.start_ms
    # Minute 9 and minute 12 held live rows before the replay; 10 and 11 only late rows.
    expectations = {9: (10, 5), 10: (10, 10), 11: (10, 10), 12: (10, 5)}
    for minute, (count, late) in expectations.items():
        bucket = start + minute * MINUTE_MS
        rows = db.run(
            """SELECT device, sample_count, late_count, refresh_count FROM metric_rollup_1m
               WHERE bucket = timestamptz 'epoch' + :b * INTERVAL '1 millisecond'
                 AND metric = 'temperature'""",
            b=bucket,
        )
        assert {r[0] for r in rows} == set(DEVICES)
        for device, sample_count, late_count, refresh_count in rows:
            assert (sample_count, late_count) == (count, late), (minute, device)
            if late < count:
                # Computed from live rows first, then again when the late rows landed.
                assert refresh_count >= 2, (minute, device)


def test_session_and_sequence_events(db):
    events = Counter(dict(db.run("SELECT event, count(*) FROM session_events GROUP BY event")))
    assert events == Counter(
        nbirth=2,
        rebirth=1,
        dbirth=9,
        ndeath=2,
        ddeath=1,
        seq_gap=1,
        out_of_order=3,
        duplicate_seq=1,
    )


def test_schema_versions(db):
    rows = db.run("SELECT device, version, metrics::text FROM device_schemas ORDER BY 1, 2")
    versions = Counter(device for device, _, _ in rows)
    assert versions == Counter({"": 1, "press-01": 2, "press-02": 1, "pump-01": 1})
    v2 = next(m for d, v, m in rows if d == "press-01" and v == 2)
    assert "motor_current" in v2
    assert db.run(
        "SELECT DISTINCT schema_version FROM metric_samples WHERE metric = 'motor_current'"
    ) == [[2]]


def test_replaying_the_whole_scenario_changes_no_rows(db):
    def snapshot():
        samples = db.run("SELECT count(*), sum(value) FROM metric_samples WHERE group_id='plant1'")
        rollups = db.run(
            """SELECT device, metric, bucket, sample_count, late_count, value_avg, refresh_count
               FROM metric_rollup_1m WHERE group_id = 'plant1' ORDER BY 1, 2, 3"""
        )
        return samples, rollups

    before = snapshot()
    before_counters = counters(db)
    compose("run", "--rm", "--no-deps", "sim")
    wait_for_messages(db, 2 * SCENARIO.messages_on_the_wire)
    assert snapshot() == before
    after = counters(db)
    assert after["samples_inserted"] == before_counters["samples_inserted"]
    assert after["samples_duplicate"] - before_counters["samples_duplicate"] == VALID_SAMPLES


def test_publish_steps_are_well_formed():
    assert all(
        isinstance(p, Publish) and p.topic.startswith("spBv1.0/") for p in SCENARIO.publishes
    )
