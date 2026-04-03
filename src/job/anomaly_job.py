from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

DELAY_THRESHOLD_SECONDS = 300  # 5 minutes — trips with max arrival delay above this are flagged


def create_anomaly_sink_postgres(t_env):
    table_name = 'trip_delay_anomalies'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
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
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_updates_source_kafka(t_env):
    table_name = "anomaly_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            id VARCHAR,
            is_deleted BOOLEAN,
            trip_update ROW<
                trip ROW<trip_id VARCHAR, route_id VARCHAR, start_time VARCHAR, start_date VARCHAR>,
                stop_time_update ARRAY<ROW<
                    stop_sequence INT,
                    stop_id VARCHAR,
                    arrival ROW<delay INT, `time` BIGINT, uncertainty INT>,
                    departure ROW<delay INT, `time` BIGINT, uncertainty INT>
                >>,
                `timestamp` BIGINT
            >,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'updates-data',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'protobuf',
            'protobuf.message-class-name' = 'com.google.transit.realtime.GtfsRealtime$FeedEntity',
            'protobuf.read-default-values' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_flattened_view(t_env, source_table):
    """Unnest stop_time_update array into individual rows with proc_time carried through."""
    view_name = "flattened_stop_updates"
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW {view_name} AS
        SELECT
            t.trip_update.trip.trip_id,
            t.trip_update.trip.route_id,
            t.trip_update.trip.start_date,
            stu.stop_sequence,
            stu.stop_id,
            CASE WHEN stu.arrival IS NOT NULL THEN stu.arrival.delay ELSE 0 END AS arrival_delay,
            t.proc_time
        FROM {source_table} AS t
        CROSS JOIN UNNEST(t.trip_update.stop_time_update) AS stu(
            stop_sequence, stop_id, arrival, departure
        )
        """
    )
    return view_name


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_updates_source_kafka(t_env)
        flattened = create_flattened_view(t_env, source_table)
        sink = create_anomaly_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
            INSERT INTO {sink}
            SELECT
                trip_id,
                route_id,
                start_date,
                TUMBLE_START(proc_time, INTERVAL '5' MINUTE) AS window_start,
                TUMBLE_END(proc_time, INTERVAL '5' MINUTE)   AS window_end,
                AVG(CAST(arrival_delay AS DOUBLE))            AS avg_arrival_delay,
                MAX(arrival_delay)                            AS max_arrival_delay,
                COUNT(stop_sequence)                          AS stop_count,
                MAX(arrival_delay) > {DELAY_THRESHOLD_SECONDS} AS is_anomaly
            FROM {flattened}
            GROUP BY
                TUMBLE(proc_time, INTERVAL '5' MINUTE),
                trip_id,
                route_id,
                start_date
            """
        ).wait()

    except Exception as e:
        print("Anomaly detection job failed:", str(e))
        raise


if __name__ == '__main__':
    run()
