-- Schema for the Sparkplug B pipeline. Runs once when the database volume is created.
--
-- The image is TimescaleDB's Apache-2.0 edition (-oss tag). That edition has
-- hypertables and time_bucket but not continuous aggregates, which are under
-- the Timescale License. The 1-minute rollup table below is therefore kept by
-- refresh_rollup_1m(), which the ingest service calls in the same transaction
-- as every insert, for exactly the buckets the inserted rows fall in.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per metric value, keyed by event time. The primary key is the
-- dedupe key: a repeated (node, device, metric, event time) is dropped.
CREATE TABLE metric_samples (
    event_time      timestamptz      NOT NULL,
    group_id        text             NOT NULL,
    edge_node       text             NOT NULL,
    device          text             NOT NULL,  -- '' for node-level metrics
    metric          text             NOT NULL,
    datatype        smallint         NOT NULL,  -- Sparkplug DataType enum value
    value           double precision,           -- numbers and booleans (0/1)
    value_text      text,                       -- strings, UUIDs, hex-encoded bytes
    is_null         boolean          NOT NULL,
    is_historical   boolean          NOT NULL,  -- flag set by the edge node
    is_late         boolean          NOT NULL,  -- behind the device watermark on arrival
    ingest_time     timestamptz      NOT NULL,
    bd_seq          smallint         NOT NULL,
    seq             smallint         NOT NULL,
    schema_version  integer          NOT NULL,
    PRIMARY KEY (group_id, edge_node, device, metric, event_time)
);

SELECT create_hypertable('metric_samples', by_range('event_time', INTERVAL '1 day'));

CREATE TABLE metric_rollup_1m (
    bucket          timestamptz      NOT NULL,
    group_id        text             NOT NULL,
    edge_node       text             NOT NULL,
    device          text             NOT NULL,
    metric          text             NOT NULL,
    sample_count    integer          NOT NULL,
    late_count      integer          NOT NULL,
    value_avg       double precision,
    value_min       double precision,
    value_max       double precision,
    refresh_count   integer          NOT NULL DEFAULT 1,
    refreshed_at    timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, edge_node, device, metric, bucket)
);

CREATE INDEX metric_rollup_1m_bucket ON metric_rollup_1m (bucket);

-- Recompute the given rollup rows from metric_samples. Each array position
-- names one (group, node, device, metric, bucket start in epoch ms). The
-- aggregate is rebuilt from raw rows rather than adjusted, so late rows,
-- dedupe and reruns all converge on the same result.
CREATE FUNCTION refresh_rollup_1m(
    p_group  text[],
    p_node   text[],
    p_device text[],
    p_metric text[],
    p_bucket bigint[]
) RETURNS integer
LANGUAGE sql AS $$
    WITH touched AS (
        SELECT DISTINCT t.group_id, t.edge_node, t.device, t.metric,
               timestamptz 'epoch' + t.bucket_ms * INTERVAL '1 millisecond' AS bucket
        FROM unnest(p_group, p_node, p_device, p_metric, p_bucket)
             AS t(group_id, edge_node, device, metric, bucket_ms)
    ), agg AS (
        SELECT t.bucket, t.group_id, t.edge_node, t.device, t.metric,
               count(*)::integer                           AS sample_count,
               count(*) FILTER (WHERE s.is_late)::integer  AS late_count,
               avg(s.value)                                AS value_avg,
               min(s.value)                                AS value_min,
               max(s.value)                                AS value_max
        FROM touched t
        JOIN metric_samples s
          ON s.group_id = t.group_id AND s.edge_node = t.edge_node
         AND s.device = t.device AND s.metric = t.metric
         AND s.event_time >= t.bucket AND s.event_time < t.bucket + INTERVAL '1 minute'
        GROUP BY t.bucket, t.group_id, t.edge_node, t.device, t.metric
    ), upserted AS (
        INSERT INTO metric_rollup_1m AS r
            (bucket, group_id, edge_node, device, metric,
             sample_count, late_count, value_avg, value_min, value_max)
        SELECT bucket, group_id, edge_node, device, metric,
               sample_count, late_count, value_avg, value_min, value_max
        FROM agg
        ON CONFLICT (group_id, edge_node, device, metric, bucket) DO UPDATE
           SET sample_count  = EXCLUDED.sample_count,
               late_count    = EXCLUDED.late_count,
               value_avg     = EXCLUDED.value_avg,
               value_min     = EXCLUDED.value_min,
               value_max     = EXCLUDED.value_max,
               refresh_count = r.refresh_count + 1,
               refreshed_at  = now()
        RETURNING 1
    )
    SELECT count(*)::integer FROM upserted;
$$;

-- Messages that could not be stored, with the raw bytes kept for replay.
CREATE TABLE dead_letters (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at  timestamptz NOT NULL,
    topic        text        NOT NULL,
    payload      bytea       NOT NULL,
    reason       text        NOT NULL,
    detail       text        NOT NULL
);

CREATE INDEX dead_letters_reason ON dead_letters (reason);

-- Session lifecycle and sequence anomalies per edge node and device.
CREATE TABLE session_events (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at  timestamptz NOT NULL,
    group_id     text        NOT NULL,
    edge_node    text        NOT NULL,
    device       text        NOT NULL,
    event        text        NOT NULL,
    bd_seq       smallint,
    seq          smallint,
    detail       text        NOT NULL
);

CREATE INDEX session_events_event ON session_events (event);

-- Metric set of each device, versioned whenever a BIRTH changes it.
CREATE TABLE device_schemas (
    group_id     text        NOT NULL,
    edge_node    text        NOT NULL,
    device       text        NOT NULL,
    version      integer     NOT NULL,
    metrics      jsonb       NOT NULL,  -- [{"name": ..., "datatype": ...}]
    first_seen   timestamptz NOT NULL,
    PRIMARY KEY (group_id, edge_node, device, version)
);

-- Running totals kept by the ingest service.
CREATE TABLE ingest_counters (
    name   text   PRIMARY KEY,
    value  bigint NOT NULL
);
