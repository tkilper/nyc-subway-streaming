from google.transit import gtfs_realtime_pb2
from src.job.anomaly_job import parse_stop_updates, DELAY_THRESHOLD_SECONDS


def _to_iso(entity):
    return entity.SerializeToString().decode('latin-1')


def _make_update_entity(trip_id="T1", route_id="R1", start_date="20240101", stops=None):
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = "E1"
    entity.trip_update.trip.trip_id = trip_id
    entity.trip_update.trip.route_id = route_id
    entity.trip_update.trip.start_date = start_date
    for stop in (stops or []):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = stop.get('stop_sequence', 0)
        stu.stop_id = stop.get('stop_id', '')
        if 'arrival_delay' in stop:
            stu.arrival.delay = stop['arrival_delay']
    return entity


class TestParseStopUpdates:
    def test_yields_one_row_per_stop(self):
        entity = _make_update_entity(stops=[
            {'stop_sequence': 1, 'stop_id': 'S1', 'arrival_delay': 60},
            {'stop_sequence': 2, 'stop_id': 'S2', 'arrival_delay': 120},
        ])
        assert len(list(parse_stop_updates(_to_iso(entity)))) == 2

    def test_all_fields_extracted(self):
        entity = _make_update_entity(
            trip_id="T1", route_id="R1", start_date="20240101",
            stops=[{'stop_sequence': 5, 'stop_id': 'S5', 'arrival_delay': 300}],
        )
        row = list(parse_stop_updates(_to_iso(entity)))[0]
        assert row[0] == "T1"        # trip_id
        assert row[1] == "R1"        # route_id
        assert row[2] == "20240101"  # start_date
        assert row[3] == 5           # stop_sequence
        assert row[4] == "S5"        # stop_id
        assert row[5] == 300         # arrival_delay

    def test_non_trip_update_yields_nothing(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.trip.trip_id = "T1"
        assert list(parse_stop_updates(_to_iso(entity))) == []

    def test_stop_without_arrival_defaults_to_zero(self):
        entity = _make_update_entity(stops=[{'stop_sequence': 1, 'stop_id': 'S1'}])
        row = list(parse_stop_updates(_to_iso(entity)))[0]
        assert row[5] == 0  # arrival_delay


class TestDelayThreshold:
    def test_threshold_is_5_minutes_in_seconds(self):
        assert DELAY_THRESHOLD_SECONDS == 300
