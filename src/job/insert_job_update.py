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


def parse_entity(iso_string):
    from google.transit import gtfs_realtime_pb2

    raw_bytes = iso_string.encode("latin-1")
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)
    if not entity.HasField("trip_update"):
        return
    tu = entity.trip_update
    trip_id = str(tu.trip.trip_id) if tu.HasField("trip") else ""
    route_id = str(tu.trip.route_id) if tu.HasField("trip") else ""
    start_date = str(tu.trip.start_date) if tu.HasField("trip") else ""
    for stu in tu.stop_time_update:
        yield Row(
            str(entity.id),
            trip_id,
            route_id,
            start_date,
            int(stu.stop_sequence),
            str(stu.stop_id),
            int(stu.arrival.delay if stu.HasField("arrival") else 0),
            int(stu.arrival.time if stu.HasField("arrival") else 0),
            int(stu.departure.delay if stu.HasField("departure") else 0),
            int(stu.departure.time if stu.HasField("departure") else 0),
        )


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda-1:29092")
        .set_topics("updates-data")
        .set_group_id("insert-job-update")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-updates-source",
        type_info=Types.STRING(),
    )

    row_type = Types.ROW_NAMED(
        [
            "feed_entity_id",
            "trip_id",
            "route_id",
            "start_date",
            "stop_sequence",
            "stop_id",
            "arrival_delay",
            "arrival_time",
            "departure_delay",
            "departure_time",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.STRING(),
            Types.INT(),
            Types.LONG(),
            Types.INT(),
            Types.LONG(),
        ],
    )

    flat_stream = raw_stream.flat_map(parse_entity, output_type=row_type)

    flat_table = t_env.from_data_stream(
        flat_stream,
        Schema.new_builder()
        .column("feed_entity_id", "STRING")
        .column("trip_id", "STRING")
        .column("route_id", "STRING")
        .column("start_date", "STRING")
        .column("stop_sequence", "INT")
        .column("stop_id", "STRING")
        .column("arrival_delay", "INT")
        .column("arrival_time", "BIGINT")
        .column("departure_delay", "INT")
        .column("departure_time", "BIGINT")
        .build(),
    )
    t_env.create_temporary_view("flat_updates", flat_table)

    t_env.execute_sql(f"""
        CREATE TABLE processed_stop_updates (
            feed_entity_id VARCHAR,
            trip_id VARCHAR,
            route_id VARCHAR,
            start_date VARCHAR,
            stop_sequence INT,
            stop_id VARCHAR,
            arrival_delay INT,
            arrival_time BIGINT,
            departure_delay INT,
            departure_time BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'processed_stop_updates',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    try:
        t_env.execute_sql("""
            INSERT INTO processed_stop_updates
            SELECT
                feed_entity_id, trip_id, route_id, start_date,
                stop_sequence, stop_id,
                arrival_delay, arrival_time,
                departure_delay, departure_time
            FROM flat_updates
        """).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_processing()
