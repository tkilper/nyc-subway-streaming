import requests
from unittest.mock import MagicMock, patch
from google.transit import gtfs_realtime_pb2
from src.producers.send_mta_data import poll


def _make_feed(*entity_specs):
    """Build a FeedMessage from a sequence of ('vehicle'|'trip_update', trip_id) pairs."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000
    for kind, trip_id in entity_specs:
        e = feed.entity.add()
        e.id = trip_id
        if kind == 'vehicle':
            e.vehicle.trip.trip_id = trip_id
        elif kind == 'trip_update':
            e.trip_update.trip.trip_id = trip_id
    return feed


class TestPoll:
    @patch('src.producers.send_mta_data.fetch_feed')
    def test_vehicle_entity_sent_to_vehicle_topic(self, mock_fetch):
        mock_fetch.return_value = _make_feed(('vehicle', 'T1'))
        producer = MagicMock()
        poll(producer)
        topics = [call.args[0] for call in producer.send.call_args_list]
        assert topics == ['vehicle-data']

    @patch('src.producers.send_mta_data.fetch_feed')
    def test_trip_update_entity_sent_to_updates_topic(self, mock_fetch):
        mock_fetch.return_value = _make_feed(('trip_update', 'T1'))
        producer = MagicMock()
        poll(producer)
        topics = [call.args[0] for call in producer.send.call_args_list]
        assert topics == ['updates-data']

    @patch('src.producers.send_mta_data.fetch_feed')
    def test_mixed_feed_routes_to_both_topics(self, mock_fetch):
        mock_fetch.return_value = _make_feed(('vehicle', 'T1'), ('trip_update', 'T2'))
        producer = MagicMock()
        poll(producer)
        topics = [call.args[0] for call in producer.send.call_args_list]
        assert 'vehicle-data' in topics
        assert 'updates-data' in topics

    @patch('src.producers.send_mta_data.fetch_feed')
    def test_producer_flushed_after_each_poll(self, mock_fetch):
        mock_fetch.return_value = _make_feed(('vehicle', 'T1'))
        producer = MagicMock()
        poll(producer)
        producer.flush.assert_called_once()

    @patch('src.producers.send_mta_data.time.sleep')
    @patch('src.producers.send_mta_data.fetch_feed')
    def test_request_exception_retried_on_first_failure(self, mock_fetch, mock_sleep):
        good_feed = _make_feed(('vehicle', 'T1'))
        mock_fetch.side_effect = [requests.exceptions.RequestException("timeout"), good_feed]
        producer = MagicMock()
        poll(producer)
        topics = [call.args[0] for call in producer.send.call_args_list]
        assert 'vehicle-data' in topics
        mock_sleep.assert_called_once()

    @patch('src.producers.send_mta_data.time.sleep')
    @patch('src.producers.send_mta_data.fetch_feed')
    def test_request_exception_on_both_tries_skips_poll(self, mock_fetch, mock_sleep):
        mock_fetch.side_effect = requests.exceptions.RequestException("timeout")
        producer = MagicMock()
        poll(producer)  # must not raise
        producer.send.assert_not_called()
        producer.flush.assert_not_called()
