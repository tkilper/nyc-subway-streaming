CREATE TABLE processed_vehicle (
    id VARCHAR,
    is_deleted BOOLEAN,
    trip_id VARCHAR,
    route_id VARCHAR,
    start_time VARCHAR,
    start_date VARCHAR,
    stop_id VARCHAR,
    current_stop_sequence INT,
    current_status INT,
    event_timestamp BIGINT
);

-- current_status values: 0 = INCOMING_AT, 1 = STOPPED_AT, 2 = IN_TRANSIT_TO

CREATE TABLE processed_stop_updates (
    feed_entity_id VARCHAR,
    trip_id VARCHAR,
    route_id VARCHAR,
    start_date VARCHAR,
    stop_sequence INT,
    stop_id VARCHAR,
    arrival_delay INT,
    arrival_time BIGINT,
    departure_delay INT,
    departure_time BIGINT
);

-- arrival_delay / departure_delay are in seconds (positive = late, negative = early)
-- arrival_time / departure_time are Unix timestamps (seconds)

CREATE TABLE trip_delay_anomalies (
    trip_id VARCHAR,
    route_id VARCHAR,
    start_date VARCHAR,
    stop_id VARCHAR,
    stop_sequence INT,
    arrival_delay INT,
    predicted_delay DOUBLE PRECISION,
    residual DOUBLE PRECISION,
    is_anomaly BOOLEAN
);

-- Populated by anomaly_job.py — one row per stop event, keyed by route_id.
-- ARIMA(1,1,1) is fit on a rolling 200-observation history per route.
-- is_anomaly = true when |arrival_delay - predicted_delay| > 3 * std(residuals).

CREATE TABLE trip_tracking (
    trip_id VARCHAR,
    route_id VARCHAR,
    start_date VARCHAR,
    current_stop_id VARCHAR,
    current_stop_sequence INT,
    current_status INT,
    arrival_delay INT,
    departure_delay INT,
    event_timestamp BIGINT
);

-- Populated by trip_tracking_job.py — interval join of vehicle positions + stop updates.
-- current_status: 0 = INCOMING_AT, 1 = STOPPED_AT, 2 = IN_TRANSIT_TO
