import sys
from unittest.mock import MagicMock


def pytest_configure(config):
    """Mock all pyflink modules before any test files are imported.

    This lets us import and test the parse functions without a Flink runtime or
    a Java installation.  Row is replaced with a plain tuple subclass so field
    access in tests works via index, matching how the real pyflink.common.Row
    behaves when iterated.

    KeyedProcessFunction is set to a real Python class rather than a MagicMock
    so that subclasses (e.g. ARIMAnomalyDetector) are proper classes with
    working __init__ and method dispatch.
    """
    class MockRow(tuple):
        def __new__(cls, *args):
            return tuple.__new__(cls, args)

    class _KeyedProcessFunction:
        pass

    pyflink_datastream = MagicMock()
    pyflink_datastream.KeyedProcessFunction = _KeyedProcessFunction

    sys.modules['pyflink'] = MagicMock()
    sys.modules['pyflink.common'] = MagicMock()
    sys.modules['pyflink.datastream'] = pyflink_datastream
    sys.modules['pyflink.datastream.connectors'] = MagicMock()
    sys.modules['pyflink.datastream.connectors.kafka'] = MagicMock()
    sys.modules['pyflink.datastream.state'] = MagicMock()
    sys.modules['pyflink.common.serialization'] = MagicMock()
    sys.modules['pyflink.common.typeinfo'] = MagicMock()
    sys.modules['pyflink.common.watermark_strategy'] = MagicMock()
    sys.modules['pyflink.table'] = MagicMock()

    sys.modules['pyflink.common'].Row = MockRow
