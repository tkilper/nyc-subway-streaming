from google.transit import gtfs_realtime_pb2
from src.job.insert_job_update import parse_entity


def _to_iso(entity):
    return entity.SerializeToString().decode('latin-1')


def _make_update_entity(entity_id="E1", trip_id="T1", route_id="R1",
                        start_date="20240101", stops=None):
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = entity_id
    entity.trip_update.trip.trip_id = trip_id
    entity.trip_update.trip.route_id = route_id
    entity.trip_update.trip.start_date = start_date
    for stop in (stops or []):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = stop.get('stop_sequence', 0)
        stu.stop_id = stop.get('stop_id', '')
        if 'arrival_delay' in stop or 'arrival_time' in stop:
            stu.arrival.delay = stop.get('arrival_delay', 0)
            stu.arrival.time = stop.get('arrival_time', 0)
        if 'departure_delay' in stop or 'departure_time' in stop:
            stu.departure.delay = stop.get('departure_delay', 0)
            stu.departure.time = stop.get('departure_time', 0)
    return entity


class TestParseEntity:
    def test_yields_one_row_per_stop(self):
        entity = _make_update_entity(stops=[
            {'stop_sequence': 1, 'stop_id': 'S1'},
            {'stop_sequence': 2, 'stop_id': 'S2'},
        ])
        assert len(list(parse_entity(_to_iso(entity)))) == 2

    def test_all_fields_extracted(self):
        entity = _make_update_entity(
            entity_id="E1", trip_id="T1", route_id="R1", start_date="20240101",
            stops=[{
                'stop_sequence': 3, 'stop_id': 'S3',
                'arrival_delay': 120, 'arrival_time': 1700000100,
                'departure_delay': 60, 'departure_time': 1700000200,
            }],
        )
        row = list(parse_entity(_to_iso(entity)))[0]
        assert row[0] == "E1"           # feed_entity_id
        assert row[1] == "T1"           # trip_id
        assert row[2] == "R1"           # route_id
        assert row[3] == "20240101"     # start_date
        assert row[4] == 3              # stop_sequence
        assert row[5] == "S3"           # stop_id
        assert row[6] == 120            # arrival_delay
        assert row[7] == 1700000100     # arrival_time
        assert row[8] == 60             # departure_delay
        assert row[9] == 1700000200     # departure_time

    def test_non_trip_update_entity_yields_nothing(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.trip.trip_id = "T1"
        assert list(parse_entity(_to_iso(entity))) == []

    def test_stop_without_arrival_or_departure_defaults_to_zero(self):
        entity = _make_update_entity(stops=[{'stop_sequence': 1, 'stop_id': 'S1'}])
        row = list(parse_entity(_to_iso(entity)))[0]
        assert row[6] == 0   # arrival_delay
        assert row[7] == 0   # arrival_time
        assert row[8] == 0   # departure_delay
        assert row[9] == 0   # departure_time

    def test_no_stops_yields_nothing(self):
        entity = _make_update_entity(stops=[])
        assert list(parse_entity(_to_iso(entity))) == []
