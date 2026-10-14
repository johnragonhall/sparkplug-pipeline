"""TimescaleDB writer for processor outcomes.

A batch is written in one transaction: samples (ON CONFLICT DO NOTHING on the
dedupe key), the rollup refresh for the buckets the inserted rows touch, dead
letters, session events, schema versions and counter increments. Either all of
it lands or none of it does, so a retry after a failure cannot double count.
"""

import contextlib
import json
from dataclasses import dataclass, field

import pg8000.native

from .model import (
    DeadLetter,
    Outcome,
    Sample,
    SchemaVersion,
    SessionEvent,
    dedupe_batch,
    touched_buckets,
)
from .processor import Signature

_EPOCH_MS = "timestamptz 'epoch' + {} * INTERVAL '1 millisecond'"

_INSERT_SAMPLES = f"""
INSERT INTO metric_samples (
    event_time, group_id, edge_node, device, metric, datatype, value, value_text,
    is_null, is_historical, is_late, ingest_time, bd_seq, seq, schema_version)
SELECT {_EPOCH_MS.format("u.ts")}, u.g, u.n, u.d, u.m, u.dt, u.v, u.vt,
       u.is_nul, u.hist, u.late, {_EPOCH_MS.format("u.its")}, u.bd, u.sq, u.sv
FROM unnest(
    CAST(:ts AS bigint[]), CAST(:g AS text[]), CAST(:n AS text[]), CAST(:d AS text[]),
    CAST(:m AS text[]), CAST(:dt AS smallint[]), CAST(:v AS float8[]), CAST(:vt AS text[]),
    CAST(:is_nul AS boolean[]), CAST(:hist AS boolean[]), CAST(:late AS boolean[]),
    CAST(:its AS bigint[]), CAST(:bd AS smallint[]), CAST(:sq AS smallint[]),
    CAST(:sv AS integer[])
) AS u(ts, g, n, d, m, dt, v, vt, is_nul, hist, late, its, bd, sq, sv)
ON CONFLICT (group_id, edge_node, device, metric, event_time) DO NOTHING
RETURNING group_id, edge_node, device, metric,
          (extract(epoch FROM event_time) * 1000)::bigint, is_late
"""

_REFRESH = """
SELECT refresh_rollup_1m(CAST(:g AS text[]), CAST(:n AS text[]), CAST(:d AS text[]),
                         CAST(:m AS text[]), CAST(:b AS bigint[]))
"""


@dataclass
class Batch:
    samples: list[Sample] = field(default_factory=list)
    dead_letters: list[DeadLetter] = field(default_factory=list)
    events: list[SessionEvent] = field(default_factory=list)
    schemas: list[SchemaVersion] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)

    def add(self, outcome: Outcome) -> None:
        self.bump("messages_received")
        if outcome.message_type:
            self.bump(f"messages_{outcome.message_type.lower()}")
        if outcome.ignored:
            self.bump("messages_ignored")
        self.samples.extend(outcome.samples)
        self.events.extend(outcome.events)
        if outcome.dead_letter is not None:
            self.dead_letters.append(outcome.dead_letter)
            self.bump("dead_letters")
        if outcome.schema is not None:
            self.schemas.append(outcome.schema)

    def bump(self, name: str, by: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + by

    def __len__(self) -> int:
        return len(self.samples) + len(self.dead_letters) + len(self.events)

    @property
    def empty(self) -> bool:
        return not (len(self) or self.schemas or self.counters)


@dataclass(frozen=True)
class WriteResult:
    attempted: int
    inserted: int
    late_inserted: int
    buckets_refreshed: int


def _col(samples: list[Sample], attr: str) -> list:
    return [getattr(s, attr) for s in samples]


class Store:
    def __init__(self, *, host: str, port: int, database: str, user: str, password: str) -> None:
        self._params = dict(host=host, port=port, database=database, user=user, password=password)
        self.conn: pg8000.native.Connection | None = None

    def connect(self) -> None:
        self.close()
        self.conn = pg8000.native.Connection(**self._params)

    def close(self) -> None:
        if self.conn is not None:
            with contextlib.suppress(Exception):  # closing a broken connection may fail
                self.conn.close()
            self.conn = None

    def load_schemas(self) -> dict[tuple[str, str, str], tuple[int, Signature]]:
        """Latest schema version per device, so numbering survives restarts."""
        rows = self.conn.run(
            """
            SELECT DISTINCT ON (group_id, edge_node, device)
                   group_id, edge_node, device, version, metrics
            FROM device_schemas
            ORDER BY group_id, edge_node, device, version DESC
            """
        )
        result = {}
        for group_id, edge_node, device, version, metrics in rows:
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            signature = tuple(sorted((m["name"], m["datatype"]) for m in metrics))
            result[(group_id, edge_node, device)] = (version, signature)
        return result

    def write(self, batch: Batch) -> WriteResult:
        conn = self.conn
        samples, _ = dedupe_batch(batch.samples)
        conn.run("START TRANSACTION")
        try:
            inserted_rows = self._insert_samples(samples) if samples else []
            keys = [row[:5] for row in inserted_rows]
            late_inserted = sum(1 for row in inserted_rows if row[5])
            refreshed = self._refresh(keys) if keys else 0
            for dl in batch.dead_letters:
                conn.run(
                    "INSERT INTO dead_letters (received_at, topic, payload, reason, detail) "
                    f"VALUES ({_EPOCH_MS.format(':ts')}, :topic, :payload, :reason, :detail)",
                    ts=dl.received_ms,
                    topic=dl.topic,
                    payload=dl.payload,
                    reason=dl.reason,
                    detail=dl.detail,
                )
            for ev in batch.events:
                conn.run(
                    "INSERT INTO session_events "
                    "(received_at, group_id, edge_node, device, event, bd_seq, seq, detail) "
                    f"VALUES ({_EPOCH_MS.format(':ts')}, :g, :n, :d, :e, :bd, :sq, :detail)",
                    ts=ev.received_ms,
                    g=ev.group_id,
                    n=ev.edge_node,
                    d=ev.device,
                    e=ev.event,
                    bd=ev.bd_seq,
                    sq=ev.seq,
                    detail=ev.detail,
                )
            for sc in batch.schemas:
                metrics = json.dumps([{"name": n, "datatype": dt} for n, dt in sc.metrics])
                conn.run(
                    "INSERT INTO device_schemas "
                    "(group_id, edge_node, device, version, metrics, first_seen) "
                    f"VALUES (:g, :n, :d, :v, CAST(:m AS jsonb), {_EPOCH_MS.format(':ts')}) "
                    "ON CONFLICT DO NOTHING",
                    g=sc.group_id,
                    n=sc.edge_node,
                    d=sc.device,
                    v=sc.version,
                    m=metrics,
                    ts=sc.first_seen_ms,
                )
            counters = dict(batch.counters)
            counters["samples_accepted"] = len(batch.samples)
            counters["samples_inserted"] = len(inserted_rows)
            counters["samples_duplicate"] = len(batch.samples) - len(inserted_rows)
            counters["samples_late_inserted"] = late_inserted
            counters["rollup_rows_refreshed"] = refreshed
            nonzero = {k: v for k, v in counters.items() if v}
            conn.run(
                "INSERT INTO ingest_counters (name, value) "
                "SELECT * FROM unnest(CAST(:names AS text[]), CAST(:deltas AS bigint[])) "
                "ON CONFLICT (name) DO UPDATE SET value = ingest_counters.value + EXCLUDED.value",
                names=list(nonzero),
                deltas=list(nonzero.values()),
            )
            conn.run("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):  # the connection itself may be gone
                conn.run("ROLLBACK")
            raise
        return WriteResult(len(batch.samples), len(inserted_rows), late_inserted, refreshed)

    def _insert_samples(self, samples: list[Sample]) -> list[list]:
        return self.conn.run(
            _INSERT_SAMPLES,
            ts=_col(samples, "event_ms"),
            g=_col(samples, "group_id"),
            n=_col(samples, "edge_node"),
            d=_col(samples, "device"),
            m=_col(samples, "metric"),
            dt=_col(samples, "datatype"),
            v=_col(samples, "value"),
            vt=_col(samples, "value_text"),
            is_nul=_col(samples, "is_null"),
            hist=_col(samples, "is_historical"),
            late=_col(samples, "is_late"),
            its=_col(samples, "ingest_ms"),
            bd=_col(samples, "bd_seq"),
            sq=_col(samples, "seq"),
            sv=_col(samples, "schema_version"),
        )

    def _refresh(self, keys) -> int:
        buckets = touched_buckets(tuple(k) for k in keys)
        g, n, d, m, b = (list(col) for col in zip(*buckets, strict=True))
        return self.conn.run(_REFRESH, g=g, n=n, d=d, m=m, b=b)[0][0]
