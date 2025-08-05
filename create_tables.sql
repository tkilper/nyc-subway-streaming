create table vehicle_positions (
    id TEXT,
    vehicle.trip.trip_id TEXT,
    vehicle.trip.route_id TEXT,
    vehicle.trip.start_time TEXT,
    vehicle.trip.start_date TEXT,
    vehicle.stop_id TEXT,
    vehicle.timestamp BIGINT
)

create table trip_updates (
    id TEXT,
    trip_update.trip.trip_id TEXT,
    trip_update.trip.route_id TEXT,
    trip_update.trip.start_time TEXT,
    trip_update.trip.start_date TEXT,
    trip_update.stop_time_update.stop_id TEXT,
    trip_update.stop_time_update.arrival.time BIGINT,
    trip_update.stop_time_update.departure.time BIGINT
)