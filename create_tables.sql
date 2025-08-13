create table processed_vehicle (
    id TEXT,
    trip_id TEXT,
    route_id TEXT,
    start_time TEXT,
    start_date TEXT,
    stop_id TEXT,
    timestamp BIGINT
)

create table processed_updates (
    id TEXT,
    trip_update.trip.trip_id TEXT,
    trip_update.trip.route_id TEXT,
    trip_update.trip.start_time TEXT,
    trip_update.trip.start_date TEXT,
    trip_update.stop_time_update.stop_id TEXT,
    trip_update.stop_time_update.arrival.time BIGINT,
    trip_update.stop_time_update.departure.time BIGINT
)