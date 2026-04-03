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
