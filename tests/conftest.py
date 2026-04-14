import sys
from unittest.mock import MagicMock


def pytest_configure(config):
    """Mock all pyflink modules before any test files are imported.

    This lets us import and test the parse functions without a Flink runtime or
    a Java installation.  Row is replaced with a plain tuple subclass so field
    access in tests works via index, matching how the real pyflink.common.Row
    behaves when iterated.
    """
    class MockRow(tuple):
        def __new__(cls, *args):
            return tuple.__new__(cls, args)

    pyflink_mocks = [
        'pyflink',
        'pyflink.common',
        'pyflink.datastream',
        'pyflink.datastream.connectors',
        'pyflink.datastream.connectors.kafka',
        'pyflink.common.serialization',
        'pyflink.common.typeinfo',
        'pyflink.common.watermark_strategy',
        'pyflink.table',
    ]
    for mod in pyflink_mocks:
        sys.modules[mod] = MagicMock()

    sys.modules['pyflink.common'].Row = MockRow
