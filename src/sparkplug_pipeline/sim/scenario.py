"""Deterministic plan for the simulated Sparkplug B edge node.

``build_scenario`` returns the exact list of MQTT actions the simulator
performs. It has no I/O, so the integration test builds the same plan and
derives its expectations from it: which rows must exist, which messages must
be dead-lettered and why, and which rows arrive late.

Timeline (one tick = 6 s of simulated event time, 10 ticks per minute):

* tick 0: NBIRTH (bdSeq 0) and a DBIRTH for each of three devices.
* ticks 1-94: one DDATA per device per tick, alias-only metrics.
  - tick 21: three tick-20 messages are published after a tick-21 message
    (out of order: one seq gap, three late arrivals).
  - tick 40: press-02's DDATA is published twice (duplicate).
  - tick 50: five invalid messages (see INVALID below).
  - tick 88: rebirth (NBIRTH with seq 0 and the same bdSeq). The seq counter
    has wrapped from 255 to 0 once before this point.
* tick 95: link outage. The node drops its connection without a clean
  DISCONNECT, so the broker publishes the NDEATH will. Ticks 95-124 are
  buffered on the node (store and forward).
* tick 125: reconnect with bdSeq 1. press-01 had a firmware update during the
  outage and its DBIRTH adds a metric, so aliases shift for every device.
* ticks 126-134: live data first, so newer buckets already hold rows.
* then the buffer is replayed as historical DDATA for ticks 92-124. Ticks
  92-94 were already delivered before the outage (the node cannot tell), so
  those rows are duplicates; ticks 95-124 are late and land in the 1-minute
  buckets of their event time.
* ticks 135-199 live. pump-01 sends DDEATH at tick 180 and one stray DDATA at
  tick 185. The node publishes NDEATH and disconnects cleanly at the end.
"""

import math
import random
from dataclasses import dataclass, field

from ..sparkplug.payload import DataType, decode_value, make_metric, make_payload

DEFAULT_START_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
TICK_MS = 6_000
GROUP_ID = "plant1"
EDGE_NODE = "edge-01"
NODE_DEVICE = ""
BUFFER_METRIC = "store_forward/depth"

BASE_METRICS = (
    ("temperature", DataType.Float),
    ("vibration", DataType.Double),
    ("cycle_count", DataType.UInt64),
)
FIRMWARE_METRIC = ("motor_current", DataType.Double)
DEVICES = ("press-01", "press-02", "pump-01")
DEVICE_BASE = {
    "press-01": (41.0, 2.1, 1000),
    "press-02": (44.0, 2.6, 5000),
    "pump-01": (35.0, 1.2, 0),
}

OUT_OF_ORDER_TICK = 20
DUPLICATE_TICK = 40
INVALID_TICK = 50
REBIRTH_TICK = 88
OUTAGE_START_TICK = 95
RECONNECT_TICK = 125
LIVE_BEFORE_REPLAY_END = 134
REPLAY_FIRST_TICK = 92
REPLAY_CHUNK_TICKS = 11
DDEATH_TICK = 180
STRAY_TICK = 185
LAST_TICK = 199


@dataclass(frozen=True, slots=True)
class ExpectedSample:
    device: str
    metric: str
    event_ms: int
    value: float
    historical: bool


@dataclass(frozen=True, slots=True)
class Publish:
    topic: str
    payload: bytes
    label: str
    expect: str = "ok"  # "ok", or the dead-letter reason the ingest must assign
    samples: tuple[ExpectedSample, ...] = ()


@dataclass(frozen=True, slots=True)
class Connect:
    bd_seq: int
    will_topic: str
    will_payload: bytes


@dataclass(frozen=True, slots=True)
class LinkLoss:
    """Drop the connection so that the broker publishes the will (an NDEATH)."""

    will_topic: str
    will_payload: bytes


@dataclass(frozen=True, slots=True)
class Close:
    """Clean disconnect."""


Step = Publish | Connect | LinkLoss | Close


@dataclass
class Scenario:
    start_ms: int
    steps: list[Step] = field(default_factory=list)

    @property
    def publishes(self) -> list[Publish]:
        return [s for s in self.steps if isinstance(s, Publish)]

    @property
    def messages_on_the_wire(self) -> int:
        """Messages the ingest service receives: every publish plus each will."""
        return len(self.publishes) + sum(isinstance(s, LinkLoss) for s in self.steps)


def reading(device: str, metric: str, tick: int) -> float | int:
    """Deterministic sensor value for a device, metric and tick."""
    temp_base, vib_base, count_base = DEVICE_BASE[device]
    phase = DEVICES.index(device)
    noise = random.Random(f"{device}/{metric}/{tick}").uniform(-1.0, 1.0)
    if metric == "temperature":
        return round(temp_base + 1.5 * math.sin(tick / 12 + phase) + 0.2 * noise, 2)
    if metric == "vibration":
        return round(vib_base + 0.3 * math.sin(tick / 7 + phase) + 0.05 * noise, 3)
    if metric == "cycle_count":
        return count_base + tick * 3 + tick % 4
    if metric == "motor_current":
        return round(12.5 + 0.8 * math.sin(tick / 9) + 0.1 * noise, 2)
    raise KeyError(metric)


class _EdgeNode:
    """Builds the messages; tracks seq, bdSeq, aliases and each device's metric set."""

    def __init__(self, start_ms: int) -> None:
        self.start_ms = start_ms
        self.seq = 0
        self.bd_seq = 0
        self.metrics: dict[str, list[tuple[str, int]]] = {d: list(BASE_METRICS) for d in DEVICES}
        self.aliases: dict[tuple[str, str], int] = {}

    def ts(self, tick: int) -> int:
        return self.start_ms + tick * TICK_MS

    def topic(self, kind: str, device: str | None = None) -> str:
        base = f"spBv1.0/{GROUP_ID}/{kind}/{EDGE_NODE}"
        return base if device is None else f"{base}/{device}"

    def next_seq(self) -> int:
        value = self.seq
        self.seq = (self.seq + 1) % 256
        return value

    def death_payload(self) -> bytes:
        metric = make_metric(name="bdSeq", datatype=DataType.Int64, value=self.bd_seq)
        return make_payload(timestamp=None, seq=None, metrics=[metric]).SerializeToString()

    def _assign_aliases(self) -> None:
        # Aliases are numbered in BIRTH order for each session, so a change in
        # one device's metric set renumbers every device after it.
        self.aliases.clear()
        next_alias = 1
        self.aliases[(NODE_DEVICE, BUFFER_METRIC)] = next_alias
        for device in DEVICES:
            for name, _ in self.metrics[device]:
                next_alias += 1
                self.aliases[(device, name)] = next_alias

    @staticmethod
    def _expected(device, name, datatype, metric, historical) -> ExpectedSample:
        value = decode_value(metric, datatype).number
        return ExpectedSample(device, name, metric.timestamp, value, historical)

    def nbirth(self, tick: int, buffer_depth: int, label: str) -> Publish:
        self.seq = 0
        self._assign_aliases()
        ts = self.ts(tick)
        depth = make_metric(
            name=BUFFER_METRIC,
            alias=self.aliases[(NODE_DEVICE, BUFFER_METRIC)],
            datatype=DataType.UInt32,
            value=buffer_depth,
            timestamp=ts,
        )
        metrics = [
            make_metric(name="bdSeq", datatype=DataType.Int64, value=self.bd_seq, timestamp=ts),
            make_metric(name="Node Control/Rebirth", datatype=DataType.Boolean, value=False),
            depth,
        ]
        payload = make_payload(timestamp=ts, seq=self.next_seq(), metrics=metrics)
        expected = (self._expected(NODE_DEVICE, BUFFER_METRIC, DataType.UInt32, depth, False),)
        return Publish(self.topic("NBIRTH"), payload.SerializeToString(), label, samples=expected)

    def ndata_buffer_depth(self, ts: int, depth: int) -> Publish:
        metric = make_metric(
            alias=self.aliases[(NODE_DEVICE, BUFFER_METRIC)],
            datatype=DataType.UInt32,
            value=depth,
            timestamp=ts,
            send_datatype=False,
        )
        payload = make_payload(timestamp=ts, seq=self.next_seq(), metrics=[metric])
        expected = (self._expected(NODE_DEVICE, BUFFER_METRIC, DataType.UInt32, metric, False),)
        return Publish(self.topic("NDATA"), payload.SerializeToString(), "ndata", samples=expected)

    def dbirth(self, device: str, tick: int, label: str) -> Publish:
        ts = self.ts(tick)
        metrics, expected = [], []
        for name, datatype in self.metrics[device]:
            metric = make_metric(
                name=name,
                alias=self.aliases[(device, name)],
                datatype=datatype,
                value=reading(device, name, tick),
                timestamp=ts,
            )
            metrics.append(metric)
            expected.append(self._expected(device, name, datatype, metric, False))
        payload = make_payload(timestamp=ts, seq=self.next_seq(), metrics=metrics)
        return Publish(
            self.topic("DBIRTH", device),
            payload.SerializeToString(),
            label,
            samples=tuple(expected),
        )

    def ddata(
        self,
        device: str,
        ticks: list[int],
        label: str = "ddata",
        historical: bool = False,
        metric_set: list[tuple[str, int]] | None = None,
    ) -> Publish:
        metrics, expected = [], []
        for tick in ticks:
            for name, datatype in metric_set or self.metrics[device]:
                metric = make_metric(
                    alias=self.aliases[(device, name)],
                    datatype=datatype,
                    value=reading(device, name, tick),
                    timestamp=self.ts(tick),
                    is_historical=historical,
                    send_datatype=False,
                )
                metrics.append(metric)
                expected.append(self._expected(device, name, datatype, metric, historical))
        payload = make_payload(timestamp=self.ts(ticks[-1]), seq=self.next_seq(), metrics=metrics)
        return Publish(
            self.topic("DDATA", device),
            payload.SerializeToString(),
            label,
            samples=tuple(expected),
        )

    def ddeath(self, device: str, tick: int) -> Publish:
        payload = make_payload(timestamp=self.ts(tick), seq=self.next_seq())
        return Publish(self.topic("DDEATH", device), payload.SerializeToString(), "ddeath")

    def invalid(self, tick: int) -> list[Publish]:
        ts = self.ts(tick)
        temp_alias = self.aliases[("press-01", "temperature")]

        def alias_only(alias, datatype, value):
            return make_metric(
                alias=alias, datatype=datatype, value=value, timestamp=ts, send_datatype=False
            )

        no_seq_payload = make_payload(
            timestamp=ts, seq=None, metrics=[alias_only(temp_alias, DataType.Float, 40.0)]
        ).SerializeToString()
        return [
            Publish(
                self.topic("DDATA", "press-01"),
                b"not a protobuf",
                "garbage bytes",
                expect="decode_error",
            ),
            Publish(
                f"spBv1.0/{GROUP_ID}/DDATA/{EDGE_NODE}",
                no_seq_payload,
                "DDATA topic without a device",
                expect="bad_topic",
            ),
            Publish(
                self.topic("DDATA", "press-01"),
                make_payload(
                    timestamp=ts,
                    seq=self.next_seq(),
                    metrics=[alias_only(999, DataType.Double, 1.0)],
                ).SerializeToString(),
                "alias not in any BIRTH",
                expect="unknown_alias",
            ),
            Publish(
                self.topic("DDATA", "ghost-01"),
                make_payload(
                    timestamp=ts,
                    seq=self.next_seq(),
                    metrics=[alias_only(temp_alias, DataType.Float, 40.0)],
                ).SerializeToString(),
                "device that never sent DBIRTH",
                expect="device_not_born",
            ),
            Publish(
                self.topic("DDATA", "press-01"),
                make_payload(
                    timestamp=ts,
                    seq=self.next_seq(),
                    metrics=[alias_only(temp_alias, DataType.String, "hot")],
                ).SerializeToString(),
                "string value for a Float metric",
                expect="type_mismatch",
            ),
        ]


def build_scenario(start_ms: int = DEFAULT_START_MS) -> Scenario:
    node = _EdgeNode(start_ms)
    sc = Scenario(start_ms)
    add = sc.steps.append

    # Session 1 ------------------------------------------------------------
    add(Connect(node.bd_seq, node.topic("NDEATH"), node.death_payload()))
    add(node.nbirth(0, buffer_depth=0, label="nbirth"))
    for device in DEVICES:
        add(node.dbirth(device, 0, "dbirth"))

    held: list[Publish] = []
    for tick in range(1, OUTAGE_START_TICK):
        if tick == REBIRTH_TICK:
            add(node.nbirth(tick, buffer_depth=0, label="rebirth"))
            for device in DEVICES:
                add(node.dbirth(device, tick, "dbirth after rebirth"))
            continue
        msgs = [node.ddata(d, [tick]) for d in DEVICES]
        if tick == OUT_OF_ORDER_TICK:
            held = msgs
            continue
        if tick == OUT_OF_ORDER_TICK + 1:
            # Natural seq order is held[0..2], msgs[0..2]. Publish msgs[0] first.
            for m in [msgs[0], held[1], held[2], held[0], msgs[1], msgs[2]]:
                add(m)
            continue
        for m in msgs:
            add(m)
        if tick == DUPLICATE_TICK:
            dup = msgs[1]
            add(Publish(dup.topic, dup.payload, "duplicate", samples=dup.samples))
        if tick == INVALID_TICK:
            for m in node.invalid(tick):
                add(m)

    # Link outage: the broker publishes the will, the node keeps measuring.
    add(LinkLoss(node.topic("NDEATH"), node.death_payload()))
    buffered_ticks = list(range(OUTAGE_START_TICK, RECONNECT_TICK))

    # Session 2 ------------------------------------------------------------
    node.bd_seq = 1
    node.metrics["press-01"] = [*BASE_METRICS, FIRMWARE_METRIC]
    add(Connect(node.bd_seq, node.topic("NDEATH"), node.death_payload()))
    add(
        node.nbirth(
            RECONNECT_TICK,
            buffer_depth=len(buffered_ticks) * len(DEVICES),
            label="nbirth after reconnect",
        )
    )
    for device in DEVICES:
        add(node.dbirth(device, RECONNECT_TICK, "dbirth after reconnect"))
    for tick in range(RECONNECT_TICK + 1, LIVE_BEFORE_REPLAY_END + 1):
        for device in DEVICES:
            add(node.ddata(device, [tick]))

    replay_ticks = list(range(REPLAY_FIRST_TICK, RECONNECT_TICK))
    for device in DEVICES:
        for i in range(0, len(replay_ticks), REPLAY_CHUNK_TICKS):
            chunk = replay_ticks[i : i + REPLAY_CHUNK_TICKS]
            add(
                node.ddata(
                    device,
                    chunk,
                    "store-and-forward replay",
                    historical=True,
                    metric_set=list(BASE_METRICS),
                )
            )
    add(node.ndata_buffer_depth(node.ts(LIVE_BEFORE_REPLAY_END) + 1_000, 0))

    pump_online = True
    for tick in range(LIVE_BEFORE_REPLAY_END + 1, LAST_TICK + 1):
        for device in DEVICES:
            if device == "pump-01":
                if tick == DDEATH_TICK:
                    add(node.ddeath(device, tick))
                    pump_online = False
                    continue
                if not pump_online:
                    if tick == STRAY_TICK:
                        stray = node.ddata(device, [tick], "DDATA after DDEATH")
                        add(
                            Publish(
                                stray.topic, stray.payload, stray.label, expect="device_offline"
                            )
                        )
                    continue
            add(node.ddata(device, [tick]))

    add(Publish(node.topic("NDEATH"), node.death_payload(), "ndeath"))
    add(Close())
    return sc
