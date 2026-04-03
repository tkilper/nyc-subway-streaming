from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_trip_tracking_sink_postgres(t_env):
    table_name = 'trip_tracking'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
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
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_vehicle_source_kafka(t_env):
    table_name = "tracking_vehicle"
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
            >,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'vehicle-data',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'protobuf',
            'protobuf.message-class-name' = 'com.google.transit.realtime.GtfsRealtime$FeedEntity',
            'protobuf.read-default-values' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_updates_source_kafka(t_env):
    table_name = "tracking_updates"
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


def create_flat_updates_view(t_env, updates_table):
    """Unnest stop_time_update so each stop is a row, keeping proc_time for interval join."""
    view_name = "flat_updates_for_tracking"
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW {view_name} AS
        SELECT
            t.trip_update.trip.trip_id       AS trip_id,
            t.trip_update.trip.start_date    AS start_date,
            stu.stop_id                      AS stop_id,
            stu.stop_sequence                AS stop_sequence,
            stu.arrival.delay                AS arrival_delay,
            stu.departure.delay              AS departure_delay,
            t.proc_time
        FROM {updates_table} AS t
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
        vehicle_table = create_vehicle_source_kafka(t_env)
        updates_table = create_updates_source_kafka(t_env)
        flat_updates = create_flat_updates_view(t_env, updates_table)
        sink = create_trip_tracking_sink_postgres(t_env)

        # Interval join: match a vehicle event to the most recent stop update for the same
        # trip + stop within a ±10-minute processing-time window.
        # This tolerates feed skew between the two topics.
        t_env.execute_sql(
            f"""
            INSERT INTO {sink}
            SELECT
                v.vehicle.trip.trip_id                AS trip_id,
                v.vehicle.trip.route_id               AS route_id,
                v.vehicle.trip.start_date             AS start_date,
                v.vehicle.stop_id                     AS current_stop_id,
                v.vehicle.current_stop_sequence       AS current_stop_sequence,
                v.vehicle.current_status              AS current_status,
                u.arrival_delay                       AS arrival_delay,
                u.departure_delay                     AS departure_delay,
                v.vehicle.`timestamp`                 AS event_timestamp
            FROM {vehicle_table} v
            JOIN {flat_updates} u
              ON  v.vehicle.trip.trip_id = u.trip_id
              AND v.vehicle.stop_id      = u.stop_id
              AND v.proc_time BETWEEN u.proc_time - INTERVAL '10' MINUTE
                                  AND u.proc_time + INTERVAL '10' MINUTE
            """
        ).wait()

    except Exception as e:
        print("Trip tracking job failed:", str(e))
        raise


if __name__ == '__main__':
    run()
