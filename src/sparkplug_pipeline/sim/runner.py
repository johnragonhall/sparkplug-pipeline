"""Runs the scenario plan against an MQTT broker.

The node waits for the primary host application's STATE message (online=true)
before its first BIRTH, as Sparkplug B v3.0.0 edge nodes do, so no data is
published before the ingest service is subscribed.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from .scenario import (
    DEFAULT_START_MS,
    EDGE_NODE,
    GROUP_ID,
    Close,
    Connect,
    LinkLoss,
    Publish,
    Scenario,
    build_scenario,
)

log = logging.getLogger("sim")

# MQTT v5 DISCONNECT reason 0x04 tells the broker to publish the will, which is
# what a dropped link looks like to everyone else on the broker.
DISCONNECT_WITH_WILL = 0x04


def _new_client(client_id: str) -> mqtt.Client:
    return mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)


class Observer:
    """Second connection that watches the primary host STATE and the node's NDEATH."""

    def __init__(self, host: str, port: int, host_id: str) -> None:
        self.state_topic = f"spBv1.0/STATE/{host_id}"
        self.death_topic = f"spBv1.0/{GROUP_ID}/NDEATH/{EDGE_NODE}"
        self.host_online = threading.Event()
        self.deaths = 0
        self._cond = threading.Condition()
        self.client = _new_client(f"{EDGE_NODE}-observer")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.connect(host, port)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        client.subscribe([(self.state_topic, 1), (self.death_topic, 0)])

    def _on_message(self, client, userdata, msg) -> None:
        if msg.topic == self.state_topic:
            try:
                online = bool(json.loads(msg.payload).get("online"))
            except (ValueError, AttributeError):
                online = False
            if online:
                self.host_online.set()
            else:
                self.host_online.clear()
        elif msg.topic == self.death_topic:
            with self._cond:
                self.deaths += 1
                self._cond.notify_all()

    def wait_for_death(self, count: int, timeout: float) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self.deaths >= count, timeout)

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


def run(
    scenario: Scenario,
    host: str,
    port: int,
    host_id: str,
    delay_s: float = 0.0,
    outage_s: float = 1.0,
) -> None:
    observer = Observer(host, port, host_id)
    log.info("waiting for primary host %s to be online", host_id)
    if not observer.host_online.wait(timeout=120):
        raise SystemExit(f"primary host {host_id} did not publish STATE online within 120 s")

    client: mqtt.Client | None = None
    deaths_seen = 0
    sent = 0
    for step in scenario.steps:
        if isinstance(step, Connect):
            client = _new_client(EDGE_NODE)
            client.will_set(step.will_topic, step.will_payload, qos=1, retain=False)
            connected = threading.Event()
            client.on_connect = lambda *_args, ev=connected: ev.set()
            client.connect(host, port, keepalive=30)
            client.loop_start()
            if not connected.wait(timeout=30):
                raise SystemExit("edge node could not connect to the broker")
            log.info("session bdSeq=%d connected", step.bd_seq)
        elif isinstance(step, Publish):
            client.publish(step.topic, step.payload, qos=0).wait_for_publish(timeout=10)
            sent += 1
            if delay_s:
                time.sleep(delay_s)
        elif isinstance(step, LinkLoss):
            client.disconnect(
                reasoncode=ReasonCode(PacketTypes.DISCONNECT, identifier=DISCONNECT_WITH_WILL)
            )
            client.loop_stop()
            deaths_seen += 1
            # Wait until the broker has delivered the will before reconnecting,
            # so subscribers see NDEATH before the next NBIRTH.
            if not observer.wait_for_death(deaths_seen, timeout=30):
                raise SystemExit("broker did not publish the NDEATH will")
            log.info("link down: broker published NDEATH; buffering for %.1f s", outage_s)
            time.sleep(outage_s)
        elif isinstance(step, Close):
            client.disconnect()
            client.loop_stop()
    observer.close()
    log.info("scenario complete: %d messages published", sent)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start = os.environ.get("SIM_START")
    start_ms = int(datetime.fromisoformat(start).timestamp() * 1000) if start else DEFAULT_START_MS
    run(
        build_scenario(start_ms),
        host=os.environ.get("MQTT_HOST", "localhost"),
        port=int(os.environ.get("MQTT_PORT", "1883")),
        host_id=os.environ.get("SPARKPLUG_HOST_ID", "ingest-01"),
        delay_s=float(os.environ.get("SIM_DELAY_MS", "2")) / 1000,
    )


if __name__ == "__main__":
    main()
