"""Turns raw MQTT messages into samples, session events and dead letters.

The processor holds the host-side Sparkplug session state for every edge node:
the current bdSeq, the sequence tracker, the alias table built from BIRTH
certificates and the online state of each device. It performs no I/O; the
service feeds it messages in arrival order and hands its outcomes to the store.
"""

from dataclasses import dataclass, field

from ..sparkplug.payload import (
    DataType,
    DecodeError,
    MetricValueError,
    Payload,
    decode_payload,
    decode_value,
)
from ..sparkplug.topic import TopicError, is_state_topic, parse_topic
from .model import DeadLetter, Outcome, Sample, SchemaVersion, SessionEvent
from .seq import SeqStatus, SeqTracker

NODE_DEVICE = ""  # device column value for node-level metrics
BD_SEQ_METRIC = "bdSeq"
CONTROL_PREFIX = "Node Control/"

SchemaKey = tuple[str, str, str]
Signature = tuple[tuple[str, int], ...]


class Reject(Exception):
    """Internal signal: the message goes to the dead-letter table with this reason."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class MetricDef:
    device: str
    name: str
    datatype: int


@dataclass(slots=True)
class DeviceState:
    online: bool
    metrics: dict[str, int]  # name -> datatype
    schema_version: int


@dataclass(slots=True)
class NodeSession:
    bd_seq: int
    seq: SeqTracker
    online: bool = True
    aliases: dict[int, MetricDef] = field(default_factory=dict)
    devices: dict[str, DeviceState] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProcessorConfig:
    allowed_lateness_ms: int = 10_000
    max_future_ms: int = 300_000


class Processor:
    def __init__(
        self,
        config: ProcessorConfig | None = None,
        schemas: dict[SchemaKey, tuple[int, Signature]] | None = None,
    ) -> None:
        self.config = config or ProcessorConfig()
        self.sessions: dict[tuple[str, str], NodeSession] = {}
        # Latest known schema per device, loaded from the database at startup.
        self.schemas: dict[SchemaKey, tuple[int, Signature]] = dict(schemas or {})
        # Highest event time accepted per device; rows behind it are late.
        self.watermarks: dict[SchemaKey, int] = {}

    # Entry point -----------------------------------------------------------

    def handle(self, topic: str, data: bytes, received_ms: int) -> Outcome:
        outcome = Outcome()
        if is_state_topic(topic):
            outcome.ignored = True
            return outcome
        try:
            self._handle(topic, data, received_ms, outcome)
        except Reject as rej:
            outcome.dead_letter = DeadLetter(
                received_ms, topic, bytes(data), rej.reason, rej.detail
            )
            outcome.samples.clear()
        return outcome

    def _handle(self, topic: str, data: bytes, received_ms: int, outcome: Outcome) -> None:
        try:
            t = parse_topic(topic)
        except TopicError as exc:
            raise Reject("bad_topic", str(exc)) from exc
        outcome.message_type = t.message_type
        if t.message_type in ("NCMD", "DCMD"):
            outcome.ignored = True  # commands flow from host applications to nodes
            return
        try:
            payload = decode_payload(data)
        except DecodeError as exc:
            raise Reject("decode_error", f"protobuf decode failed: {exc}") from exc

        node_key = (t.group_id, t.edge_node)
        if t.message_type == "NBIRTH":
            self._nbirth(t.group_id, t.edge_node, payload, received_ms, outcome)
            return
        if t.message_type == "NDEATH":
            self._ndeath(t.group_id, t.edge_node, payload, received_ms, outcome)
            return

        session = self.sessions.get(node_key)
        if session is None or not session.online:
            raise Reject("no_session", f"{t.message_type} from a node without a live NBIRTH")
        if not payload.HasField("seq"):
            raise Reject("missing_seq", f"{t.message_type} payload has no seq")
        self._track_seq(
            t.group_id, t.edge_node, t.device, session, payload.seq, received_ms, outcome
        )

        if t.message_type == "DBIRTH":
            self._dbirth(t.group_id, t.edge_node, t.device, session, payload, received_ms, outcome)
        elif t.message_type == "DDEATH":
            self._ddeath(t.group_id, t.edge_node, t.device, session, payload, received_ms, outcome)
        elif t.message_type == "DDATA":
            device = session.devices.get(t.device)
            if device is None:
                raise Reject("device_not_born", f"no DBIRTH for {t.device} in this session")
            if not device.online:
                raise Reject("device_offline", f"{t.device} sent DDATA after DDEATH")
            self._data(
                t.group_id, t.edge_node, t.device, device, session, payload, received_ms, outcome
            )
        else:  # NDATA
            node = session.devices[NODE_DEVICE]
            self._data(
                t.group_id, t.edge_node, NODE_DEVICE, node, session, payload, received_ms, outcome
            )

    # Sequence numbers ------------------------------------------------------

    def _track_seq(self, group_id, edge_node, device, session, seq, received_ms, outcome) -> None:
        status, skipped = session.seq.observe(seq)
        if status is SeqStatus.IN_ORDER:
            return
        detail = f"{skipped} seq values skipped" if status is SeqStatus.GAP else ""
        event = {
            SeqStatus.GAP: "seq_gap",
            SeqStatus.OUT_OF_ORDER: "out_of_order",
            SeqStatus.DUPLICATE: "duplicate_seq",
        }[status]
        outcome.events.append(
            SessionEvent(
                received_ms,
                group_id,
                edge_node,
                device or NODE_DEVICE,
                event,
                session.bd_seq,
                seq,
                detail,
            )
        )

    # BIRTH and DEATH -------------------------------------------------------

    @staticmethod
    def _bd_seq(payload: Payload, message_type: str) -> int:
        for metric in payload.metrics:
            if metric.name == BD_SEQ_METRIC:
                if metric.WhichOneof("value") != "long_value":
                    raise Reject("invalid_birth", f"{message_type} bdSeq is not an Int64/UInt64")
                return metric.long_value
        raise Reject("invalid_birth", f"{message_type} has no bdSeq metric")

    def _nbirth(self, group_id, edge_node, payload, received_ms, outcome) -> None:
        if not payload.HasField("seq") or payload.seq != 0:
            raise Reject("invalid_birth", "NBIRTH seq must be 0")
        bd_seq = self._bd_seq(payload, "NBIRTH")
        defs = self._birth_defs(NODE_DEVICE, payload)
        prior = self.sessions.get((group_id, edge_node))
        session = NodeSession(bd_seq=bd_seq, seq=SeqTracker(0))
        self._register_aliases(session, NODE_DEVICE, payload)
        node_metrics = {
            name: dt
            for name, dt in defs.items()
            if name != BD_SEQ_METRIC and not name.startswith(CONTROL_PREFIX)
        }
        version, schema = self._schema(group_id, edge_node, NODE_DEVICE, node_metrics, received_ms)
        state = DeviceState(True, node_metrics, version)
        session.devices[NODE_DEVICE] = state
        self._data(
            group_id,
            edge_node,
            NODE_DEVICE,
            state,
            session,
            payload,
            received_ms,
            outcome,
            by_name=True,
            skip=lambda name: name not in node_metrics,
        )
        # The BIRTH is valid: it replaces all state for the node, including its devices.
        self.sessions[(group_id, edge_node)] = session
        self._commit_schema(schema, outcome)
        rebirth = prior is not None and prior.online and prior.bd_seq == bd_seq
        outcome.events.append(
            SessionEvent(
                received_ms,
                group_id,
                edge_node,
                NODE_DEVICE,
                "rebirth" if rebirth else "nbirth",
                bd_seq,
                0,
            )
        )

    def _ndeath(self, group_id, edge_node, payload, received_ms, outcome) -> None:
        bd_seq = self._bd_seq(payload, "NDEATH")
        session = self.sessions.get((group_id, edge_node))
        if session is None or not session.online or session.bd_seq != bd_seq:
            current = None if session is None else session.bd_seq
            outcome.events.append(
                SessionEvent(
                    received_ms,
                    group_id,
                    edge_node,
                    NODE_DEVICE,
                    "stale_ndeath",
                    bd_seq,
                    None,
                    f"current bdSeq {current}",
                )
            )
            return
        session.online = False
        for device in session.devices.values():
            device.online = False
        outcome.events.append(
            SessionEvent(received_ms, group_id, edge_node, NODE_DEVICE, "ndeath", bd_seq, None)
        )

    def _dbirth(self, group_id, edge_node, device, session, payload, received_ms, outcome) -> None:
        defs = self._birth_defs(device, payload)
        self._check_alias_conflicts(session, device, payload)
        version, schema = self._schema(group_id, edge_node, device, defs, received_ms)
        state = DeviceState(True, defs, version)
        # Values are validated before any state changes so a bad DBIRTH leaves no trace.
        self._data(
            group_id, edge_node, device, state, session, payload, received_ms, outcome, by_name=True
        )
        session.devices[device] = state
        self._register_aliases(session, device, payload)
        self._commit_schema(schema, outcome)
        outcome.events.append(
            SessionEvent(
                received_ms,
                group_id,
                edge_node,
                device,
                "dbirth",
                session.bd_seq,
                payload.seq,
                f"schema v{version}",
            )
        )

    def _ddeath(self, group_id, edge_node, device, session, payload, received_ms, outcome) -> None:
        state = session.devices.get(device)
        if state is None:
            raise Reject("device_not_born", f"DDEATH for {device} without a DBIRTH")
        state.online = False
        outcome.events.append(
            SessionEvent(
                received_ms, group_id, edge_node, device, "ddeath", session.bd_seq, payload.seq
            )
        )

    @staticmethod
    def _birth_defs(device: str, payload: Payload) -> dict[str, int]:
        defs: dict[str, int] = {}
        aliases: set[int] = set()
        for index, metric in enumerate(payload.metrics):
            if metric.HasField("alias"):
                if metric.alias in aliases:
                    raise Reject("alias_conflict", f"BIRTH uses alias {metric.alias} twice")
                aliases.add(metric.alias)
            if not metric.name:
                raise Reject("invalid_birth", f"BIRTH metric {index} has no name")
            if not metric.HasField("datatype"):
                raise Reject("invalid_birth", f"BIRTH metric {metric.name!r} has no datatype")
            if metric.name in defs:
                raise Reject("invalid_birth", f"BIRTH lists {metric.name!r} twice")
            defs[metric.name] = metric.datatype
        return defs

    @staticmethod
    def _check_alias_conflicts(session: NodeSession, device: str, payload: Payload) -> None:
        for metric in payload.metrics:
            if not metric.HasField("alias"):
                continue
            existing = session.aliases.get(metric.alias)
            if existing is not None and (existing.device, existing.name) != (device, metric.name):
                raise Reject(
                    "alias_conflict",
                    f"alias {metric.alias} already names {existing.device}/{existing.name}",
                )

    def _register_aliases(self, session: NodeSession, device: str, payload: Payload) -> None:
        self._check_alias_conflicts(session, device, payload)
        for metric in payload.metrics:
            if metric.HasField("alias"):
                session.aliases[metric.alias] = MetricDef(device, metric.name, metric.datatype)

    def _schema(
        self, group_id, edge_node, device, defs, received_ms
    ) -> tuple[int, SchemaVersion | None]:
        """Return the schema version for a BIRTH's metric set, and a new record if it changed."""
        signature: Signature = tuple(sorted(defs.items()))
        known = self.schemas.get((group_id, edge_node, device))
        if known is not None and known[1] == signature:
            return known[0], None
        version = 1 if known is None else known[0] + 1
        return version, SchemaVersion(group_id, edge_node, device, version, signature, received_ms)

    def _commit_schema(self, schema: SchemaVersion | None, outcome: Outcome) -> None:
        if schema is None:
            return
        key = (schema.group_id, schema.edge_node, schema.device)
        self.schemas[key] = (schema.version, schema.metrics)
        outcome.schema = schema

    # Data ------------------------------------------------------------------

    @staticmethod
    def _resolve(
        session: NodeSession, device: str, state: DeviceState, metric, by_name: bool
    ) -> tuple[str, int]:
        if metric.HasField("alias") and not by_name:
            definition = session.aliases.get(metric.alias)
            if definition is None:
                raise Reject("unknown_alias", f"alias {metric.alias} is not in any BIRTH")
            if definition.device != device:
                raise Reject(
                    "unknown_alias",
                    f"alias {metric.alias} belongs to {definition.device or 'the node'}",
                )
            if metric.name and metric.name != definition.name:
                raise Reject(
                    "alias_mismatch",
                    f"alias {metric.alias} is {definition.name!r}, not {metric.name!r}",
                )
            name, datatype = definition.name, definition.datatype
        elif metric.name:
            if metric.name not in state.metrics:
                raise Reject("unknown_metric", f"{metric.name!r} is not in the BIRTH")
            name, datatype = metric.name, state.metrics[metric.name]
        else:
            raise Reject("unknown_metric", "metric has neither name nor alias")
        if metric.HasField("datatype") and metric.datatype != datatype:
            raise Reject(
                "type_mismatch",
                f"{name!r} was born as {DataType.Name(datatype)}, message says {metric.datatype}",
            )
        return name, datatype

    def _data(
        self,
        group_id,
        edge_node,
        device,
        state,
        session,
        payload,
        received_ms,
        outcome,
        by_name: bool = False,
        skip=None,
    ) -> None:
        payload_ts = payload.timestamp if payload.HasField("timestamp") else None
        seq = payload.seq if payload.HasField("seq") else 0
        wm_key = (group_id, edge_node, device)
        watermark = self.watermarks.get(wm_key)
        samples: list[Sample] = []
        for metric in payload.metrics:
            if skip is not None and skip(metric.name):
                continue
            name, datatype = self._resolve(session, device, state, metric, by_name)
            try:
                value = decode_value(metric, datatype)
            except MetricValueError as exc:
                raise Reject(exc.reason, f"{name!r}: {exc}") from exc
            event_ms = metric.timestamp if metric.HasField("timestamp") else payload_ts
            if event_ms is None:
                raise Reject("missing_timestamp", f"{name!r} has no timestamp")
            if event_ms > received_ms + self.config.max_future_ms:
                raise Reject("future_timestamp", f"{name!r} is {event_ms - received_ms} ms ahead")
            is_late = (
                watermark is not None and event_ms < watermark - self.config.allowed_lateness_ms
            )
            if watermark is None or event_ms > watermark:
                watermark = event_ms
            samples.append(
                Sample(
                    event_ms=event_ms,
                    group_id=group_id,
                    edge_node=edge_node,
                    device=device,
                    metric=name,
                    datatype=datatype,
                    value=value.number,
                    value_text=value.text,
                    is_null=value.is_null,
                    is_historical=metric.is_historical,
                    is_late=is_late,
                    ingest_ms=received_ms,
                    bd_seq=session.bd_seq,
                    seq=seq,
                    schema_version=state.schema_version,
                )
            )
        # Only a fully valid message moves the watermark and emits rows.
        if watermark is not None:
            self.watermarks[wm_key] = watermark
        outcome.samples.extend(samples)
