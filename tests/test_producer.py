import requests
from unittest.mock import MagicMock, patch
from google.transit import gtfs_realtime_pb2
from src.producers.send_mta_data import poll, FEED_URLS


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


@patch('src.producers.send_mta_data.FEED_URLS', ['http://feed-1'])
class TestPollSingleFeed:
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


class TestPollAllFeeds:
    def test_feed_urls_cover_all_subway_feeds(self):
        # The eight NYC subway GTFS-realtime feeds (numbered lines, ACE, BDFM,
        # G, JZ, NQRW, L, Staten Island Railway).
        assert len(FEED_URLS) == 8
        suffixes = {u.rsplit('/', 1)[-1] for u in FEED_URLS}
        assert suffixes == {
            'nyct%2Fgtfs',
            'nyct%2Fgtfs-ace',
            'nyct%2Fgtfs-bdfm',
            'nyct%2Fgtfs-g',
            'nyct%2Fgtfs-jz',
            'nyct%2Fgtfs-nqrw',
            'nyct%2Fgtfs-l',
            'nyct%2Fgtfs-si',
        }

    @patch('src.producers.send_mta_data.fetch_feed')
    def test_poll_fetches_every_feed(self, mock_fetch):
        mock_fetch.return_value = _make_feed()
        producer = MagicMock()
        poll(producer)
        fetched_urls = [call.args[0] for call in mock_fetch.call_args_list]
        assert fetched_urls == FEED_URLS

    @patch('src.producers.send_mta_data.FEED_URLS', ['http://feed-a', 'http://feed-b'])
    @patch('src.producers.send_mta_data.fetch_feed')
    def test_entities_from_all_feeds_published(self, mock_fetch):
        mock_fetch.side_effect = [
            _make_feed(('vehicle', 'A1')),
            _make_feed(('trip_update', 'B1')),
        ]
        producer = MagicMock()
        poll(producer)
        sent = [(call.args[0], call.kwargs['value'].id) for call in producer.send.call_args_list]
        assert ('vehicle-data', 'A1') in sent
        assert ('updates-data', 'B1') in sent

    @patch('src.producers.send_mta_data.time.sleep')
    @patch('src.producers.send_mta_data.FEED_URLS', ['http://feed-a', 'http://feed-b'])
    @patch('src.producers.send_mta_data.fetch_feed')
    def test_one_feed_failing_does_not_block_others(self, mock_fetch, mock_sleep):
        mock_fetch.side_effect = [
            requests.exceptions.RequestException("down"),   # feed-a, first try
            requests.exceptions.RequestException("down"),   # feed-a, retry
            _make_feed(('vehicle', 'B1')),                  # feed-b
        ]
        producer = MagicMock()
        poll(producer)
        sent = [(call.args[0], call.kwargs['value'].id) for call in producer.send.call_args_list]
        assert sent == [('vehicle-data', 'B1')]
        producer.flush.assert_called_once()
