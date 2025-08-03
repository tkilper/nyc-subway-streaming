create table vehicle_positions (
    id VARCHAR
    vehicle.trip.trip_id
    vehicle.trip.route_id
    vehicle.trip.start_time
    vehicle.trip.start_date
    vehicle.stop_id
    vehicle.timestamp
)

create table trip_updates (
    id VARCHAR
    trip_update.trip.trip_id
    trip_update.trip.route_id
    trip_update.trip.start_time
    trip_update.trip.start_date
    trip_update.stop_time_update.stop_id
    trip_update.stop_time_update.arrival
    trip_update.stop_time_update.departure
)