create table processed_vehicle (
    id VARCHAR,
    is_deleted BOOLEAN,
    trip_id VARCHAR, 
    route_id VARCHAR, 
    start_time VARCHAR, 
    start_date VARCHAR, 
    stop_id VARCHAR, 
    event_timestamp BIGINT
)

create table processed_updates (
    id VARCHAR,
    trip_update VARCHAR
)