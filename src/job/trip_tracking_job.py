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
        str(v.trip.trip_id) if v.HasField("trip") else "",
        str(v.trip.route_id) if v.HasField("trip") else "",
        str(v.trip.start_date) if v.HasField("trip") else "",
        str(v.stop_id),
        int(v.current_stop_sequence),
        int(v.current_status),
        int(v.timestamp),
    )


def parse_stop_updates(iso_string):
    from google.transit import gtfs_realtime_pb2

    raw_bytes = iso_string.encode("latin-1")
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)
    if not entity.HasField("trip_update"):
        return
    tu = entity.trip_update
    trip_id = str(tu.trip.trip_id) if tu.HasField("trip") else ""
    start_date = str(tu.trip.start_date) if tu.HasField("trip") else ""
    for stu in tu.stop_time_update:
        arrival_delay = int(stu.arrival.delay if stu.HasField("arrival") else 0)
        departure_delay = int(stu.departure.delay if stu.HasField("departure") else 0)
        yield Row(
            trip_id,
            start_date,
            str(stu.stop_id),
            int(stu.stop_sequence),
            arrival_delay,
            departure_delay,
        )


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # --- Vehicle source ---
    vehicle_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda-1:29092")
        .set_topics("vehicle-data")
        .set_group_id("trip-tracking-vehicle")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )
    raw_vehicle = env.from_source(
        vehicle_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-vehicle-source",
        type_info=Types.STRING(),
    )
    vehicle_row_type = Types.ROW_NAMED(
        [
            "trip_id",
            "route_id",
            "start_date",
            "stop_id",
            "current_stop_sequence",
            "current_status",
            "event_timestamp",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.LONG(),
        ],
    )
    vehicle_stream = raw_vehicle.flat_map(parse_vehicle, output_type=vehicle_row_type)

    # --- Updates source ---
    updates_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda-1:29092")
        .set_topics("updates-data")
        .set_group_id("trip-tracking-updates")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )
    raw_updates = env.from_source(
        updates_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-updates-source",
        type_info=Types.STRING(),
    )
    update_row_type = Types.ROW_NAMED(
        [
            "trip_id",
            "start_date",
            "stop_id",
            "stop_sequence",
            "arrival_delay",
            "departure_delay",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
        ],
    )
    updates_stream = raw_updates.flat_map(
        parse_stop_updates, output_type=update_row_type
    )

    # Convert both DataStreams to Tables, adding proc_time for the interval join
    vehicle_table = t_env.from_data_stream(
        vehicle_stream,
        Schema.new_builder()
        .column("trip_id", "STRING")
        .column("route_id", "STRING")
        .column("start_date", "STRING")
        .column("stop_id", "STRING")
        .column("current_stop_sequence", "INT")
        .column("current_status", "INT")
        .column("event_timestamp", "BIGINT")
        .column_by_expression("proc_time", "PROCTIME()")
        .build(),
    )
    t_env.create_temporary_view("vehicle_tbl", vehicle_table)

    updates_table = t_env.from_data_stream(
        updates_stream,
        Schema.new_builder()
        .column("trip_id", "STRING")
        .column("start_date", "STRING")
        .column("stop_id", "STRING")
        .column("stop_sequence", "INT")
        .column("arrival_delay", "INT")
        .column("departure_delay", "INT")
        .column_by_expression("proc_time", "PROCTIME()")
        .build(),
    )
    t_env.create_temporary_view("updates_tbl", updates_table)

    t_env.execute_sql(f"""
        CREATE TABLE trip_tracking (
            trip_id VARCHAR,
            route_id VARCHAR,
            start_date VARCHAR,
            current_stop_id VARCHAR,
            current_stop_sequence INT,
            current_status INT,
            arrival_delay INT,
            departure_delay INT,
            event_timestamp BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'trip_tracking',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    try:
        t_env.execute_sql("""
            INSERT INTO trip_tracking
            SELECT
                v.trip_id,
                v.route_id,
                v.start_date,
                v.stop_id as current_stop_id,
                v.current_stop_sequence,
                v.current_status,
                u.arrival_delay,
                u.departure_delay,
                v.event_timestamp
            FROM vehicle_tbl v
            JOIN updates_tbl u
              ON  v.trip_id = u.trip_id
              AND v.stop_id = u.stop_id
              AND v.proc_time BETWEEN u.proc_time - INTERVAL '10' MINUTE
                                  AND u.proc_time + INTERVAL '10' MINUTE
        """).wait()
    except Exception as e:
        print("Trip tracking job failed:", str(e))
        raise


if __name__ == "__main__":
    run()
