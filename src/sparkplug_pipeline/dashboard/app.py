"""Read-only dashboard API over the telemetry database, plus the static page."""

import os
from contextlib import closing
from pathlib import Path

import pg8000.native
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC = Path(__file__).parent / "static"

app = FastAPI(title="Sparkplug pipeline dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def connect() -> pg8000.native.Connection:
    return pg8000.native.Connection(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "telemetry"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "postgres"),
    )


def query(sql: str, **params) -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.run(sql, **params)
        names = [c["name"] for c in conn.columns or []]
    return [dict(zip(names, row, strict=True)) for row in rows]


def epoch_ms(column: str) -> str:
    return f"(extract(epoch FROM {column}) * 1000)::bigint"


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/healthz")
def healthz() -> dict:
    query("SELECT 1 AS ok")
    return {"ok": True}


@app.get("/api/summary")
def summary() -> dict:
    counters = {r["name"]: r["value"] for r in query("SELECT name, value FROM ingest_counters")}
    return {
        "counters": counters,
        "dead_letters": query(
            "SELECT reason, count(*) AS count FROM dead_letters GROUP BY reason ORDER BY count DESC"
        ),
        "events": query(
            "SELECT event, count(*) AS count FROM session_events GROUP BY event ORDER BY event"
        ),
        "schemas": query(
            f"""SELECT device, version, metrics::text AS metrics,
                       {epoch_ms("first_seen")} AS first_seen
                FROM device_schemas WHERE device <> '' ORDER BY device, version"""
        ),
        "metrics": [
            r["metric"]
            for r in query(
                "SELECT DISTINCT metric FROM metric_rollup_1m WHERE device <> '' ORDER BY metric"
            )
        ],
    }


@app.get("/api/series")
def series(metric: str = Query("temperature", max_length=200)) -> dict:
    rows = query(
        f"""SELECT device, {epoch_ms("bucket")} AS bucket, value_avg, sample_count, late_count
            FROM metric_rollup_1m WHERE metric = :metric AND device <> ''
            ORDER BY bucket, device""",
        metric=metric,
    )
    if not rows:
        raise HTTPException(404, f"no rollups for metric {metric!r}")
    buckets = sorted({r["bucket"] for r in rows})
    devices = sorted({r["device"] for r in rows})
    index = {b: i for i, b in enumerate(buckets)}
    avg = {d: [None] * len(buckets) for d in devices}
    late = [0] * len(buckets)
    total = [0] * len(buckets)
    for r in rows:
        i = index[r["bucket"]]
        avg[r["device"]][i] = r["value_avg"]
        late[i] += r["late_count"]
        total[i] += r["sample_count"]
    return {
        "metric": metric,
        "buckets": [b / 1000 for b in buckets],
        "avg": avg,
        "late": late,
        "total": total,
    }


@app.get("/api/dead-letters")
def dead_letters(limit: int = Query(20, ge=1, le=200)) -> list[dict]:
    return query(
        f"""SELECT {epoch_ms("received_at")} AS received_at, topic, reason, detail,
                   length(payload) AS payload_bytes
            FROM dead_letters ORDER BY id DESC LIMIT :limit""",
        limit=limit,
    )


@app.get("/api/events")
def events(limit: int = Query(30, ge=1, le=500)) -> list[dict]:
    return query(
        f"""SELECT {epoch_ms("received_at")} AS received_at, device, event, bd_seq, seq, detail
            FROM session_events
            WHERE event NOT IN ('dbirth')
            ORDER BY id DESC LIMIT :limit""",
        limit=limit,
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("DASHBOARD_HOST", "0.0.0.0"),
        port=int(os.environ.get("DASHBOARD_PORT", "8080")),
    )


if __name__ == "__main__":
    main()
