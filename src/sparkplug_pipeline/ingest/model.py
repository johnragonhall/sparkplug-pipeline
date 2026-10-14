"""Records produced by the processor and written by the store."""

from collections.abc import Iterable
from dataclasses import dataclass, field

MINUTE_MS = 60_000

SampleKey = tuple[str, str, str, str, int]  # group, node, device, metric, event ms


def minute_bucket(event_ms: int) -> int:
    """Start of the one-minute bucket that holds ``event_ms`` (epoch milliseconds)."""
    return event_ms - event_ms % MINUTE_MS


@dataclass(frozen=True, slots=True)
class Sample:
    event_ms: int
    group_id: str
    edge_node: str
    device: str  # "" for node-level metrics
    metric: str
    datatype: int
    value: float | None
    value_text: str | None
    is_null: bool
    is_historical: bool
    is_late: bool
    ingest_ms: int
    bd_seq: int
    seq: int
    schema_version: int

    @property
    def key(self) -> SampleKey:
        return (self.group_id, self.edge_node, self.device, self.metric, self.event_ms)


@dataclass(frozen=True, slots=True)
class DeadLetter:
    received_ms: int
    topic: str
    payload: bytes
    reason: str
    detail: str


@dataclass(frozen=True, slots=True)
class SessionEvent:
    received_ms: int
    group_id: str
    edge_node: str
    device: str
    event: str
    bd_seq: int | None
    seq: int | None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SchemaVersion:
    group_id: str
    edge_node: str
    device: str
    version: int
    metrics: tuple[tuple[str, int], ...]  # sorted (name, datatype) pairs
    first_seen_ms: int


@dataclass(slots=True)
class Outcome:
    """Everything one MQTT message produced."""

    message_type: str | None = None
    samples: list[Sample] = field(default_factory=list)
    events: list[SessionEvent] = field(default_factory=list)
    dead_letter: DeadLetter | None = None
    schema: SchemaVersion | None = None
    ignored: bool = False


def touched_buckets(keys: Iterable[SampleKey]) -> list[SampleKey]:
    """Map sample keys to the distinct rollup buckets they fall in, by event time.

    Each result is (group_id, edge_node, device, metric, bucket_start_ms). The
    store recomputes exactly these rollup rows after an insert, so a late
    sample refreshes the bucket of its event time, not of its arrival time.
    """
    return sorted({(g, n, d, m, minute_bucket(ms)) for g, n, d, m, ms in keys})


def dedupe_batch(samples: list[Sample]) -> tuple[list[Sample], int]:
    """Keep the first sample for each key. Returns the kept samples and the number dropped.

    The database enforces the same key across batches; this removes repeats
    inside one batch before they reach it.
    """
    seen: set[tuple[str, str, str, str, int]] = set()
    kept: list[Sample] = []
    for sample in samples:
        key = sample.key
        if key in seen:
            continue
        seen.add(key)
        kept.append(sample)
    return kept, len(samples) - len(kept)
