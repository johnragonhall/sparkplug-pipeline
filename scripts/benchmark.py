"""End-to-end ingest throughput against the running compose stack.

    uv run scripts/benchmark.py --messages 20000

Method: publish one NBIRTH and one DBIRTH, then N DDATA messages (one device,
three metrics each, event times 1 s apart) as fast as the client can send them
at QoS 0 to the broker's host port. The clock starts before the first DDATA
and stops when the ingest_counters table shows every message received and the
transaction that holds the last one committed. The result covers decode,
validation, dedupe, the sample insert and the rollup refresh. It uses group
"bench" so it does not touch the scenario data.
"""

import argparse
import time

import paho.mqtt.client as mqtt
import pg8000.native
from paho.mqtt.enums import CallbackAPIVersion

from sparkplug_pipeline.sparkplug.payload import DataType, make_metric, make_payload

GROUP, NODE, DEVICE = "bench", "bench-node", "bench-dev"
START_MS = 1_769_904_000_000  # 2026-02-01T00:00:00Z
METRICS = (
    ("temperature", DataType.Float),
    ("vibration", DataType.Double),
    ("cycle_count", DataType.UInt64),
)


def counter(conn, name: str) -> int:
    rows = conn.run("SELECT value FROM ingest_counters WHERE name = :n", n=name)
    return rows[0][0] if rows else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--messages", type=int, default=20_000)
    ap.add_argument("--mqtt-port", type=int, default=18830)
    ap.add_argument("--db-port", type=int, default=15433)
    ap.add_argument(
        "--run", type=int, default=0, help="offsets event times so reruns are not deduped"
    )
    args = ap.parse_args()

    db = pg8000.native.Connection(
        host="127.0.0.1",
        port=args.db_port,
        database="telemetry",
        user="postgres",
        password="sparkplug-demo",
    )
    before_msgs = counter(db, "messages_received")
    before_rows = counter(db, "samples_inserted")
    start_ms = START_MS + args.run * 86_400_000

    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="bench-node", protocol=mqtt.MQTTv5)
    client.max_queued_messages_set(0)
    client.connect("127.0.0.1", args.mqtt_port)
    client.loop_start()

    seq = 0

    def publish(kind, payload, device=None):
        topic = f"spBv1.0/{GROUP}/{kind}/{NODE}" + (f"/{device}" if device else "")
        return client.publish(topic, payload.SerializeToString(), qos=0)

    publish(
        "NBIRTH",
        make_payload(
            timestamp=start_ms,
            seq=0,
            metrics=[make_metric(name="bdSeq", datatype=DataType.Int64, value=args.run)],
        ),
    ).wait_for_publish()
    seq = 1
    publish(
        "DBIRTH",
        make_payload(
            timestamp=start_ms,
            seq=seq,
            metrics=[
                make_metric(name=n, alias=i + 1, datatype=dt, value=0, timestamp=start_ms)
                for i, (n, dt) in enumerate(METRICS)
            ],
        ),
        DEVICE,
    ).wait_for_publish()

    payloads = []
    for i in range(1, args.messages + 1):
        seq = (seq + 1) % 256
        ts = start_ms + i * 1000
        payloads.append(
            make_payload(
                timestamp=ts,
                seq=seq,
                metrics=[
                    make_metric(
                        alias=1,
                        datatype=DataType.Float,
                        value=40 + i % 7,
                        timestamp=ts,
                        send_datatype=False,
                    ),
                    make_metric(
                        alias=2,
                        datatype=DataType.Double,
                        value=2.0 + i % 5 / 10,
                        timestamp=ts,
                        send_datatype=False,
                    ),
                    make_metric(
                        alias=3,
                        datatype=DataType.UInt64,
                        value=i,
                        timestamp=ts,
                        send_datatype=False,
                    ),
                ],
            )
        )

    t0 = time.perf_counter()
    last = None
    for p in payloads:
        last = publish("DDATA", p, DEVICE)
    last.wait_for_publish()
    t_pub = time.perf_counter() - t0

    target = before_msgs + args.messages + 2
    while counter(db, "messages_received") < target:
        if time.perf_counter() - t0 > 600:
            raise SystemExit("timed out waiting for the ingest service")
        time.sleep(0.05)
    elapsed = time.perf_counter() - t0
    rows = counter(db, "samples_inserted") - before_rows
    client.disconnect()
    client.loop_stop()

    print(f"messages: {args.messages} DDATA ({rows} sample rows inserted)")
    print(f"publish time: {t_pub:.2f} s ({args.messages / t_pub:,.0f} msg/s offered)")
    print(
        f"end-to-end: {elapsed:.2f} s -> {args.messages / elapsed:,.0f} msg/s, "
        f"{rows / elapsed:,.0f} rows/s"
    )


if __name__ == "__main__":
    main()
