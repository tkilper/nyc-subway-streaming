from google.transit import gtfs_realtime_pb2
from src.job.insert_job_vehicle import parse_vehicle


def _to_iso(entity):
    return entity.SerializeToString().decode('latin-1')


def _make_vehicle_entity(entity_id="E1", is_deleted=False, trip_id="T1",
                         route_id="R1", start_time="08:00:00",
                         start_date="20240101", stop_id="S1",
                         stop_sequence=5, status=1, timestamp=1700000000):
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = entity_id
    entity.is_deleted = is_deleted
    entity.vehicle.trip.trip_id = trip_id
    entity.vehicle.trip.route_id = route_id
    entity.vehicle.trip.start_time = start_time
    entity.vehicle.trip.start_date = start_date
    entity.vehicle.stop_id = stop_id
    entity.vehicle.current_stop_sequence = stop_sequence
    entity.vehicle.current_status = status
    entity.vehicle.timestamp = timestamp
    return entity


class TestParseVehicle:
    def test_yields_one_row_for_vehicle_entity(self):
        rows = list(parse_vehicle(_to_iso(_make_vehicle_entity())))
        assert len(rows) == 1

    def test_all_fields_extracted(self):
        entity = _make_vehicle_entity(
            entity_id="E1", trip_id="T1", route_id="R1",
            start_time="08:00:00", start_date="20240101",
            stop_id="S1", stop_sequence=5, status=1, timestamp=1700000000,
        )
        row = list(parse_vehicle(_to_iso(entity)))[0]
        assert row == ("E1", False, "T1", "R1", "08:00:00", "20240101", "S1", 5, 1, 1700000000)

    def test_non_vehicle_entity_yields_nothing(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.trip_update.trip.trip_id = "T1"
        assert list(parse_vehicle(_to_iso(entity))) == []

    def test_vehicle_without_trip_uses_empty_strings(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.stop_id = "S1"
        entity.vehicle.current_stop_sequence = 3
        entity.vehicle.current_status = 0
        entity.vehicle.timestamp = 100
        row = list(parse_vehicle(_to_iso(entity)))[0]
        assert row[2] == ""  # trip_id
        assert row[3] == ""  # route_id
        assert row[4] == ""  # start_time
        assert row[5] == ""  # start_date

    def test_is_deleted_flag(self):
        entity = _make_vehicle_entity(is_deleted=True)
        row = list(parse_vehicle(_to_iso(entity)))[0]
        assert row[1] is True
