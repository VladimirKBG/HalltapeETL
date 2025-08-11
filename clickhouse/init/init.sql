CREATE TABLE IF NOT EXISTS default.enriched_earthquakes (
    id String,
    ts DateTime,
    place String,
    region String,
    magnitude Float32,
    felt Nullable(Int32),
    tsunami Int32,
    url String,
    longitude Float64,
    latitude Float64,
    depth Float64,
    load_date Date
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, region);


CREATE TABLE default.earth_quake_agg
(

    `place_hash` String,

    `count` Int32
)
ENGINE = SummingMergeTree
ORDER BY place_hash
SETTINGS index_granularity = 8192;

CREATE TABLE default.earth_quake_full
(

    `id` String,

    `ts` DateTime,

    `load_date` Date,

    `magnitude` Float64,

    `felt` Int64,

    `tsunami` Int64,

    `url` String,

    `longitude` Float64,

    `latitude` Float64,

    `depth` Float64,

    `place_hash` String,

    `updated_at` DateTime,

    `load_to_ch_utc` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(updated_at)
ORDER BY (load_date,
 id)
SETTINGS index_granularity = 8192;
