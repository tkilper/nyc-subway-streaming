from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_vehicle'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            id VARCHAR,
            trip_id VARCHAR,
            route_id VARCHAR,
            start_time VARCHAR,
            start_date VARCHAR,
            stop_id VARCHAR,
            ts BIGINT
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
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            id VARCHAR,
            is_deleted BOOLEAN,
            trip_update TripUpdate,
            vehicle VehiclePosition,
            alert Alert
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'vehicle-data',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'protobuf',
            'protobuf.message-class-name' = 'GtfsRealtime$FeedEntity'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        # write records to postgres too!
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        id,
                        is_deleted,
                        trip_update,
                        vehicle,
                        alert
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_processing()