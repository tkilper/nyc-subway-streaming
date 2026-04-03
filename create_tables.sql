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
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_arrival_delay DOUBLE PRECISION,
    max_arrival_delay INT,
    stop_count BIGINT,
    is_anomaly BOOLEAN
);

-- Populated by anomaly_job.py — 5-minute tumbling windows per trip.
-- is_anomaly = true when max_arrival_delay > 300 seconds (5 min).

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
