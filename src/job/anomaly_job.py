from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, Schema
from pyflink.common import Row

DELAY_THRESHOLD_SECONDS = 300  # 5 minutes


def parse_stop_updates(iso_string):
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
        arrival_delay = int(stu.arrival.delay if stu.HasField("arrival") else 0)
        yield Row(
            trip_id,
            route_id,
            start_date,
            int(stu.stop_sequence),
            str(stu.stop_id),
            arrival_delay,
        )


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda-1:29092")
        .set_topics("updates-data")
        .set_group_id("anomaly-job")
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
            "trip_id",
            "route_id",
            "start_date",
            "stop_sequence",
            "stop_id",
            "arrival_delay",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.STRING(),
            Types.INT(),
        ],
    )

    flat_stream = raw_stream.flat_map(parse_stop_updates, output_type=row_type)

    # Convert DataStream to Table, adding proc_time as a time attribute for windowing
    flat_table = t_env.from_data_stream(
        flat_stream,
        Schema.new_builder()
        .column("trip_id", "STRING")
        .column("route_id", "STRING")
        .column("start_date", "STRING")
        .column("stop_sequence", "INT")
        .column("stop_id", "STRING")
        .column("arrival_delay", "INT")
        .column_by_expression("proc_time", "PROCTIME()")
        .build(),
    )
    t_env.create_temporary_view("flat_updates", flat_table)

    t_env.execute_sql("""
        CREATE TABLE trip_delay_anomalies (
            trip_id VARCHAR,
            route_id VARCHAR,
            start_date VARCHAR,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            avg_arrival_delay DOUBLE,
            max_arrival_delay INT,
            stop_count BIGINT,
            is_anomaly BOOLEAN
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trip_delay_anomalies',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    try:
        t_env.execute_sql(f"""
            INSERT INTO trip_delay_anomalies
            SELECT
                trip_id,
                route_id,
                start_date,
                TUMBLE_START(proc_time, INTERVAL '5' MINUTE) as window_start,
                TUMBLE_END(proc_time, INTERVAL '5' MINUTE) as window_end,
                AVG(CAST(arrival_delay AS DOUBLE)) as avg_arrival_delay,
                MAX(arrival_delay) as max_arrival_delay,
                COUNT(stop_sequence) as stop_count,
                MAX(arrival_delay) > {DELAY_THRESHOLD_SECONDS} as is_anomaly
            FROM flat_updates
            GROUP BY
                TUMBLE(proc_time, INTERVAL '5' MINUTE),
                trip_id,
                route_id,
                start_date
        """).wait()
    except Exception as e:
        print("Anomaly detection job failed:", str(e))
        raise


if __name__ == "__main__":
    run()
