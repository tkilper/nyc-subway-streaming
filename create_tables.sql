create table processed_vehicle (
    id VARCHAR,
    trip_id VARCHAR,
    route_id VARCHAR,
    start_time VARCHAR,
    start_date VARCHAR,
    stop_id VARCHAR,
    timestamp BIGINT
)

create table processed_updates (
    id VARCHAR,
    trip_id VARCHAR,
    route_id VARCHAR,
    start_time VARCHAR,
    start_date VARCHAR,
    stop_id VARCHAR,
    arrival_time BIGINT,
    departure_time BIGINT
)