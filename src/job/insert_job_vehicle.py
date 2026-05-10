import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, Schema
from pyflink.common import Row

PG_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/postgres")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")


def parse_vehicle(iso_string):
    from google.transit import gtfs_realtime_pb2

    raw_bytes = iso_string.encode("latin-1")
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)
    if not entity.HasField("vehicle"):
        return
    v = entity.vehicle
    yield Row(
        str(entity.id),
        bool(entity.is_deleted),
        str(v.trip.trip_id) if v.HasField("trip") else "",
        str(v.trip.route_id) if v.HasField("trip") else "",
        str(v.trip.start_time) if v.HasField("trip") else "",
        str(v.trip.start_date) if v.HasField("trip") else "",
        str(v.stop_id),
        int(v.current_stop_sequence),
        int(v.current_status),
        int(v.timestamp),
    )


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda-1:29092")
        .set_topics("vehicle-data")
        .set_group_id("insert-job-vehicle")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-vehicle-source",
        type_info=Types.STRING(),
    )

    row_type = Types.ROW_NAMED(
        [
            "id",
            "is_deleted",
            "trip_id",
            "route_id",
            "start_time",
            "start_date",
            "stop_id",
            "current_stop_sequence",
            "current_status",
            "event_timestamp",
        ],
        [
            Types.STRING(),
            Types.BOOLEAN(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.LONG(),
        ],
    )

    parsed = raw_stream.flat_map(parse_vehicle, output_type=row_type)

    parsed_table = t_env.from_data_stream(
        parsed,
        Schema.new_builder()
        .column("id", "STRING")
        .column("is_deleted", "BOOLEAN")
        .column("trip_id", "STRING")
        .column("route_id", "STRING")
        .column("start_time", "STRING")
        .column("start_date", "STRING")
        .column("stop_id", "STRING")
        .column("current_stop_sequence", "INT")
        .column("current_status", "INT")
        .column("event_timestamp", "BIGINT")
        .build(),
    )
    t_env.create_temporary_view("parsed_vehicle", parsed_table)

    t_env.execute_sql(f"""
        CREATE TABLE processed_vehicle (
            id VARCHAR,
            is_deleted BOOLEAN,
            trip_id VARCHAR,
            route_id VARCHAR,
            start_time VARCHAR,
            start_date VARCHAR,
            stop_id VARCHAR,
            current_stop_sequence INT,
            current_status INT,
            event_timestamp BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'processed_vehicle',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    try:
        t_env.execute_sql("""
            INSERT INTO processed_vehicle
            SELECT
                id, is_deleted, trip_id, route_id, start_time, start_date,
                stop_id, current_stop_sequence, current_status, event_timestamp
            FROM parsed_vehicle
        """).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_processing()
