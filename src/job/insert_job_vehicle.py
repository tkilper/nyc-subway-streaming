from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_vehicle'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
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
    table_name = "events_vehicle"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            id VARCHAR,
            is_deleted BOOLEAN,
            vehicle ROW<
                trip ROW<trip_id VARCHAR, route_id VARCHAR, start_time VARCHAR, start_date VARCHAR>,
                stop_id VARCHAR,
                current_stop_sequence INT,
                current_status INT,
                `timestamp` BIGINT
            >
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'vehicle-data',
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
        postgres_sink = create_processed_events_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                id,
                is_deleted,
                vehicle.trip.trip_id,
                vehicle.trip.route_id,
                vehicle.trip.start_time,
                vehicle.trip.start_date,
                vehicle.stop_id,
                vehicle.current_stop_sequence,
                vehicle.current_status,
                vehicle.`timestamp` AS event_timestamp
            FROM {source_table}
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
