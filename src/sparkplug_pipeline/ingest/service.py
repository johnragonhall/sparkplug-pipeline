"""Ingest service: MQTT subscriber feeding the processor and the TimescaleDB store.

The paho network thread only timestamps each message and puts it on a queue.
A single worker thread decodes and processes messages in arrival order, which
the per-node sequence tracking depends on, and writes them in batches.
"""

import json
import logging
import os
import queue
import signal
import tempfile
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pg8000.exceptions
from paho.mqtt.enums import CallbackAPIVersion

from .processor import Processor, ProcessorConfig
from .store import Batch, Store

log = logging.getLogger("ingest")

SUBSCRIPTION = "spBv1.0/#"
CONNECTION_ERRORS = (pg8000.exceptions.InterfaceError, OSError)


# The readiness marker may only live under the temp directory.
READY_FILE_BASE = Path(tempfile.gettempdir())
DEFAULT_READY_FILE = "ingest-ready"


def confined_path(value: str, base: Path) -> Path:
    """Resolve ``value`` (relative paths are taken from ``base``) and require it inside ``base``.

    Raises ValueError for anything that resolves outside ``base``, such as a
    ``..`` segment or an absolute path elsewhere, so a bad setting stops the
    service at startup instead of writing to an arbitrary location.
    """
    root = base.resolve()
    candidate = (root / value).resolve()
    if candidate == root or not candidate.is_relative_to(root):
        raise ValueError(f"path {value!r} resolves outside {root}; refusing to use it")
    return candidate


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class IngestService:
    def __init__(self) -> None:
        self.host_id = os.environ.get("SPARKPLUG_HOST_ID", "ingest-01")
        self.state_topic = f"spBv1.0/STATE/{self.host_id}"
        self.mqtt_host = os.environ.get("MQTT_HOST", "localhost")
        self.mqtt_port = env_int("MQTT_PORT", 1883)
        self.batch_max = env_int("INGEST_BATCH_MAX", 5000)
        self.flush_ms = env_int("INGEST_FLUSH_MS", 200)
        self.ready_file = confined_path(
            os.environ.get("INGEST_READY_FILE", DEFAULT_READY_FILE), READY_FILE_BASE
        )
        self.store = Store(
            host=os.environ.get("DB_HOST", "localhost"),
            port=env_int("DB_PORT", 5432),
            database=os.environ.get("DB_NAME", "telemetry"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "postgres"),
        )
        self.config = ProcessorConfig(
            allowed_lateness_ms=env_int("INGEST_ALLOWED_LATENESS_MS", 10_000),
            max_future_ms=env_int("INGEST_MAX_FUTURE_MS", 300_000),
        )
        self.queue: queue.SimpleQueue[tuple[str, bytes, int]] = queue.SimpleQueue()
        self.stop = threading.Event()
        self.client: mqtt.Client | None = None
        self.totals = {"messages": 0, "inserted": 0, "duplicates": 0, "dead_letters": 0}

    # MQTT ------------------------------------------------------------------

    def _state_payload(self, online: bool) -> str:
        return json.dumps({"online": online, "timestamp": now_ms()})

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            log.error("MQTT connect refused: %s", reason_code)
            return
        log.info(
            "connected to MQTT %s:%s, subscribing to %s",
            self.mqtt_host,
            self.mqtt_port,
            SUBSCRIPTION,
        )
        client.subscribe(SUBSCRIPTION, qos=0)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        # Sparkplug primary host: announce online only once the subscription is active,
        # so edge nodes that wait for this STATE message cannot publish into a gap.
        client.publish(self.state_topic, self._state_payload(True), qos=1, retain=True)
        self.ready_file.touch()
        log.info("published STATE online on %s", self.state_topic)

    def _on_message(self, client, userdata, msg) -> None:
        self.queue.put((msg.topic, msg.payload, now_ms()))

    def start_mqtt(self) -> None:
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=self.host_id, protocol=mqtt.MQTTv5
        )
        client.will_set(self.state_topic, self._state_payload(False), qos=1, retain=True)
        client.on_connect = self._on_connect
        client.on_subscribe = self._on_subscribe
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        while not self.stop.is_set():
            try:
                client.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
                break
            except OSError as exc:
                log.warning("MQTT broker not reachable (%s); retrying", exc)
                time.sleep(1)
        client.loop_start()
        self.client = client

    # Database --------------------------------------------------------------

    def connect_db(self) -> None:
        while not self.stop.is_set():
            try:
                self.store.connect()
                return
            except CONNECTION_ERRORS as exc:
                log.warning("database not reachable (%s); retrying", exc)
                time.sleep(1)

    def flush(self, batch: Batch) -> None:
        delay = 0.5
        while True:
            try:
                result = self.store.write(batch)
                break
            except CONNECTION_ERRORS as exc:
                log.warning("database write failed (%s); reconnecting", exc)
                time.sleep(delay)
                delay = min(delay * 2, 10)
                self.connect_db()
        self.totals["messages"] += batch.counters.get("messages_received", 0)
        self.totals["inserted"] += result.inserted
        self.totals["duplicates"] += result.attempted - result.inserted
        self.totals["dead_letters"] += len(batch.dead_letters)

    # Main loop -------------------------------------------------------------

    def run(self) -> None:
        self.ready_file.unlink(missing_ok=True)
        self.connect_db()
        processor = Processor(self.config, schemas=self.store.load_schemas())
        log.info("loaded %d device schemas", len(processor.schemas))
        self.start_mqtt()

        batch = Batch()
        batch_started = 0.0
        last_report = time.monotonic()
        while not self.stop.is_set():
            timeout = self.flush_ms / 1000
            try:
                topic, data, received_ms = self.queue.get(timeout=timeout)
                if batch.empty:
                    batch_started = time.monotonic()
                batch.add(processor.handle(topic, data, received_ms))
            except queue.Empty:
                pass
            if not batch.empty and (
                len(batch.samples) >= self.batch_max
                or self.queue.empty()
                or (time.monotonic() - batch_started) * 1000 >= self.flush_ms
            ):
                self.flush(batch)
                batch = Batch()
            if time.monotonic() - last_report >= 10:
                last_report = time.monotonic()
                log.info("totals %s", self.totals)

        if not batch.empty:
            self.flush(batch)
        if self.client is not None:
            self.client.publish(self.state_topic, self._state_payload(False), qos=1, retain=True)
            self.client.disconnect()
            self.client.loop_stop()
        self.store.close()
        log.info("stopped; totals %s", self.totals)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    service = IngestService()
    signal.signal(signal.SIGTERM, lambda *_: service.stop.set())
    signal.signal(signal.SIGINT, lambda *_: service.stop.set())
    service.run()


if __name__ == "__main__":
    main()
