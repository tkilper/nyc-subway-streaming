from google.transit import gtfs_realtime_pb2
from src.job.trip_tracking_job import parse_vehicle, parse_stop_updates


def _to_iso(entity):
    return entity.SerializeToString().decode('latin-1')


class TestParseVehicle:
    def test_all_fields_extracted(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.trip.trip_id = "T1"
        entity.vehicle.trip.route_id = "R1"
        entity.vehicle.trip.start_date = "20240101"
        entity.vehicle.stop_id = "S1"
        entity.vehicle.current_stop_sequence = 5
        entity.vehicle.current_status = 1
        entity.vehicle.timestamp = 1700000000
        row = list(parse_vehicle(_to_iso(entity)))[0]
        assert row == ("T1", "R1", "20240101", "S1", 5, 1, 1700000000)

    def test_non_vehicle_entity_yields_nothing(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.trip_update.trip.trip_id = "T1"
        assert list(parse_vehicle(_to_iso(entity))) == []

    def test_vehicle_without_trip_uses_empty_strings(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.stop_id = "S1"
        entity.vehicle.current_stop_sequence = 1
        entity.vehicle.current_status = 0
        entity.vehicle.timestamp = 100
        row = list(parse_vehicle(_to_iso(entity)))[0]
        assert row[0] == ""  # trip_id
        assert row[1] == ""  # route_id
        assert row[2] == ""  # start_date


class TestParseStopUpdates:
    def test_yields_one_row_per_stop(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.trip_update.trip.trip_id = "T1"
        entity.trip_update.trip.start_date = "20240101"
        for i in range(3):
            stu = entity.trip_update.stop_time_update.add()
            stu.stop_sequence = i
            stu.stop_id = f"S{i}"
        assert len(list(parse_stop_updates(_to_iso(entity)))) == 3

    def test_all_fields_with_arrival_and_departure(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.trip_update.trip.trip_id = "T1"
        entity.trip_update.trip.start_date = "20240101"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = 3
        stu.stop_id = "S3"
        stu.arrival.delay = 90
        stu.departure.delay = 45
        row = list(parse_stop_updates(_to_iso(entity)))[0]
        assert row[0] == "T1"        # trip_id
        assert row[1] == "20240101"  # start_date
        assert row[2] == "S3"        # stop_id
        assert row[3] == 3           # stop_sequence
        assert row[4] == 90          # arrival_delay
        assert row[5] == 45          # departure_delay

    def test_stop_without_delays_defaults_to_zero(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.trip_update.trip.trip_id = "T1"
        entity.trip_update.trip.start_date = "20240101"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = 1
        stu.stop_id = "S1"
        row = list(parse_stop_updates(_to_iso(entity)))[0]
        assert row[4] == 0  # arrival_delay
        assert row[5] == 0  # departure_delay
