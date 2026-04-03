from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import (
    JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Row


def parse_entity(iso_string):
    from google.transit import gtfs_realtime_pb2
    raw_bytes = iso_string.encode('latin-1')
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)
    if not entity.HasField('trip_update'):
        return
    tu = entity.trip_update
    trip_id    = str(tu.trip.trip_id)    if tu.HasField('trip') else ''
    route_id   = str(tu.trip.route_id)   if tu.HasField('trip') else ''
    start_date = str(tu.trip.start_date) if tu.HasField('trip') else ''
    for stu in tu.stop_time_update:
        yield Row(
            str(entity.id),
            trip_id,
            route_id,
            start_date,
            int(stu.stop_sequence),
            str(stu.stop_id),
            int(stu.arrival.delay   if stu.HasField('arrival')   else 0),
            int(stu.arrival.time    if stu.HasField('arrival')   else 0),
            int(stu.departure.delay if stu.HasField('departure') else 0),
            int(stu.departure.time  if stu.HasField('departure') else 0),
        )


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers('redpanda-1:29092')
        .set_topics('updates-data')
        .set_group_id('insert-job-update')
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema('ISO-8859-1'))
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        'kafka-updates-source',
        type_info=Types.STRING()
    )

    row_type = Types.ROW_NAMED(
        ['feed_entity_id', 'trip_id', 'route_id', 'start_date',
         'stop_sequence', 'stop_id',
         'arrival_delay', 'arrival_time',
         'departure_delay', 'departure_time'],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
         Types.INT(), Types.STRING(),
         Types.INT(), Types.INT(),
         Types.INT(), Types.INT()]
    )

    flattened = raw_stream.flat_map(parse_entity, output_type=row_type)

    jdbc_sink = JdbcSink.sink(
        """INSERT INTO processed_stop_updates
           (feed_entity_id, trip_id, route_id, start_date,
            stop_sequence, stop_id,
            arrival_delay, arrival_time,
            departure_delay, departure_time)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        row_type,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url('jdbc:postgresql://postgres:5432/postgres')
        .with_driver_name('org.postgresql.Driver')
        .with_user_name('postgres')
        .with_password('postgres')
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(3)
        .build()
    )

    flattened.add_sink(jdbc_sink)

    try:
        env.execute('insert-job-update')
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
