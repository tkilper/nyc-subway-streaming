from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_stop_updates_sink_postgres(t_env):
    table_name = 'processed_stop_updates'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
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
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "events_updates"
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
            >
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'updates-data',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'protobuf',
            'protobuf.message-class-name' = 'GtfsRealtime$FeedEntity',
            'protobuf.read-default-values' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_stop_updates_sink_postgres(t_env)

        # UNNEST the repeated stop_time_update field so each stop becomes its own row
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                id AS feed_entity_id,
                trip_update.trip.trip_id,
                trip_update.trip.route_id,
                trip_update.trip.start_date,
                stu.stop_sequence,
                stu.stop_id,
                stu.arrival.delay AS arrival_delay,
                stu.arrival.`time` AS arrival_time,
                stu.departure.delay AS departure_delay,
                stu.departure.`time` AS departure_time
            FROM {source_table}
            CROSS JOIN UNNEST(trip_update.stop_time_update) AS stu(
                stop_sequence, stop_id, arrival, departure
            )
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
