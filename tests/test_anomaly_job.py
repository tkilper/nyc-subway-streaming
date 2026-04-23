import numpy as np
from unittest.mock import patch, MagicMock
from google.transit import gtfs_realtime_pb2
from src.job.anomaly_job import (
    parse_stop_updates,
    ARIMAnomalyDetector,
    MIN_OBSERVATIONS,
    MAX_OBSERVATIONS,
)


# ── parse_stop_updates helpers ────────────────────────────────────────────────

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


# ── ARIMAnomalyDetector helpers ───────────────────────────────────────────────

class _ListState:
    def __init__(self):
        self._items = []

    def get(self):
        return iter(self._items)

    def update(self, lst):
        self._items = list(lst)


class _RuntimeContext:
    def get_list_state(self, _):
        return _ListState()


def _make_detector(prefill=None):
    d = ARIMAnomalyDetector()
    d.open(_RuntimeContext())
    if prefill:
        d._history.update(prefill)
    return d


def _make_value(trip_id="T1", route_id="R1", start_date="20240101",
                stop_sequence=1, stop_id="S1", arrival_delay=60):
    return (trip_id, route_id, start_date, stop_sequence, stop_id, arrival_delay)


# ── Tests: parse_stop_updates ─────────────────────────────────────────────────

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
        assert row[0] == "T1"
        assert row[1] == "R1"
        assert row[2] == "20240101"
        assert row[3] == 5
        assert row[4] == "S5"
        assert row[5] == 300

    def test_non_trip_update_yields_nothing(self):
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.id = "E1"
        entity.vehicle.trip.trip_id = "T1"
        assert list(parse_stop_updates(_to_iso(entity))) == []

    def test_stop_without_arrival_defaults_to_zero(self):
        entity = _make_update_entity(stops=[{'stop_sequence': 1, 'stop_id': 'S1'}])
        row = list(parse_stop_updates(_to_iso(entity)))[0]
        assert row[5] == 0


# ── Tests: ARIMAnomalyDetector ────────────────────────────────────────────────

class TestARIMAnomalyDetector:
    def test_output_has_nine_fields(self):
        d = _make_detector()
        rows = list(d.process_element(_make_value(), ctx=None))
        assert len(rows) == 1
        assert len(rows[0]) == 9

    def test_output_fields_below_threshold(self):
        # Output schema: trip_id, route_id, start_date, stop_id, stop_sequence,
        # arrival_delay, predicted_delay, residual, is_anomaly
        # Note: stop_id (3) and stop_sequence (4) are swapped vs parse_stop_updates output
        d = _make_detector()
        row = list(d.process_element(_make_value(arrival_delay=120), ctx=None))[0]
        assert row[0] == "T1"
        assert row[1] == "R1"
        assert row[2] == "20240101"
        assert row[3] == "S1"
        assert row[4] == 1
        assert row[5] == 120
        assert row[6] is None   # predicted_delay — not computed below threshold
        assert row[7] is None   # residual
        assert row[8] is False  # is_anomaly

    def test_no_arima_below_min_observations(self):
        d = _make_detector(prefill=list(range(MIN_OBSERVATIONS - 2)))
        row = list(d.process_element(_make_value(arrival_delay=100), ctx=None))[0]
        assert row[6] is None
        assert row[7] is None
        assert row[8] is False

    def test_history_accumulates_across_calls(self):
        d = _make_detector()
        for delay in [10, 20, 30]:
            list(d.process_element(_make_value(arrival_delay=delay), ctx=None))
        assert list(d._history.get()) == [10, 20, 30]

    def test_history_trimmed_to_max_observations(self):
        d = _make_detector(prefill=list(range(MAX_OBSERVATIONS)))
        list(d.process_element(_make_value(arrival_delay=999), ctx=None))
        stored = list(d._history.get())
        assert len(stored) == MAX_OBSERVATIONS
        assert stored[-1] == 999

    def test_arima_runs_above_min_observations(self):
        # Real ARIMA integration test — simple linear series so fit is stable
        prefill = list(range(MIN_OBSERVATIONS - 1))
        d = _make_detector(prefill=prefill)
        row = list(d.process_element(_make_value(arrival_delay=50), ctx=None))[0]
        assert row[6] is not None  # predicted_delay computed
        assert row[7] is not None  # residual computed

    def test_arima_exception_suppressed(self):
        d = _make_detector(prefill=list(range(MIN_OBSERVATIONS - 1)))
        with patch('statsmodels.tsa.arima.model.ARIMA', side_effect=Exception("fit failed")):
            row = list(d.process_element(_make_value(arrival_delay=500), ctx=None))[0]
        assert row[6] is None
        assert row[7] is None
        assert row[8] is False

    def test_anomaly_detected_when_residual_exceeds_sigma(self):
        d = _make_detector(prefill=list(range(MIN_OBSERVATIONS - 1)))
        mock_fit = MagicMock()
        mock_fit.forecast.return_value = np.array([100.0])
        mock_fit.resid = np.array([1.0, -1.0, 1.0, -1.0])  # std = 1.0
        with patch('statsmodels.tsa.arima.model.ARIMA') as mock_arima:
            mock_arima.return_value.fit.return_value = mock_fit
            # residual = |200 - 100| = 100 > 3.0 * 1.0 → anomaly
            row = list(d.process_element(_make_value(arrival_delay=200), ctx=None))[0]
        assert row[8] is True

    def test_no_anomaly_when_residual_within_sigma(self):
        d = _make_detector(prefill=list(range(MIN_OBSERVATIONS - 1)))
        mock_fit = MagicMock()
        mock_fit.forecast.return_value = np.array([100.0])
        mock_fit.resid = np.array([10.0, -10.0, 10.0, -10.0])  # std = 10.0
        with patch('statsmodels.tsa.arima.model.ARIMA') as mock_arima:
            mock_arima.return_value.fit.return_value = mock_fit
            # residual = |102 - 100| = 2 < 3.0 * 10.0 → no anomaly
            row = list(d.process_element(_make_value(arrival_delay=102), ctx=None))[0]
        assert row[8] is False
