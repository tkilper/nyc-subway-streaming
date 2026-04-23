from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.state import ListStateDescriptor
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, Schema
from pyflink.common import Row

MIN_OBSERVATIONS = 50
MAX_OBSERVATIONS = 200
ARIMA_ORDER = (1, 1, 1)
ANOMALY_SIGMA = 3.0


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


class ARIMAnomalyDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self._history = runtime_context.get_list_state(
            ListStateDescriptor("delay_history", Types.INT())
        )

    def process_element(self, value, ctx):
        import numpy as np
        from statsmodels.tsa.arima.model import ARIMA

        trip_id, route_id, start_date, stop_sequence, stop_id, arrival_delay = (
            value[0], value[1], value[2], value[3], value[4], value[5]
        )

        history = list(self._history.get() or [])
        history.append(arrival_delay)
        if len(history) > MAX_OBSERVATIONS:
            history = history[-MAX_OBSERVATIONS:]
        self._history.update(history)

        predicted_delay = None
        residual = None
        is_anomaly = False

        if len(history) >= MIN_OBSERVATIONS:
            try:
                series = np.array(history[:-1], dtype=float)
                fit = ARIMA(series, order=ARIMA_ORDER).fit()
                predicted_delay = float(fit.forecast(steps=1)[0])
                std_resid = float(np.std(fit.resid))
                residual = float(abs(arrival_delay - predicted_delay))
                is_anomaly = std_resid > 0 and residual > ANOMALY_SIGMA * std_resid
            except Exception:
                pass

        yield Row(
            trip_id,
            route_id,
            start_date,
            stop_id,
            stop_sequence,
            arrival_delay,
            predicted_delay,
            residual,
            is_anomaly,
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

    parsed_type = Types.ROW_NAMED(
        ["trip_id", "route_id", "start_date", "stop_sequence", "stop_id", "arrival_delay"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT()],
    )

    output_type = Types.ROW_NAMED(
        ["trip_id", "route_id", "start_date", "stop_id", "stop_sequence",
         "arrival_delay", "predicted_delay", "residual", "is_anomaly"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
         Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN()],
    )

    result_stream = (
        raw_stream
        .flat_map(parse_stop_updates, output_type=parsed_type)
        .key_by(lambda row: row[1])  # key by route_id
        .process(ARIMAnomalyDetector(), output_type=output_type)
    )

    result_table = t_env.from_data_stream(
        result_stream,
        Schema.new_builder()
        .column("trip_id", "STRING")
        .column("route_id", "STRING")
        .column("start_date", "STRING")
        .column("stop_id", "STRING")
        .column("stop_sequence", "INT")
        .column("arrival_delay", "INT")
        .column("predicted_delay", "DOUBLE")
        .column("residual", "DOUBLE")
        .column("is_anomaly", "BOOLEAN")
        .build(),
    )
    t_env.create_temporary_view("arima_results", result_table)

    t_env.execute_sql("""
        CREATE TABLE trip_delay_anomalies (
            trip_id VARCHAR,
            route_id VARCHAR,
            start_date VARCHAR,
            stop_id VARCHAR,
            stop_sequence INT,
            arrival_delay INT,
            predicted_delay DOUBLE,
            residual DOUBLE,
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
        t_env.execute_sql("""
            INSERT INTO trip_delay_anomalies SELECT * FROM arima_results
        """).wait()
    except Exception as e:
        print("ARIMA anomaly job failed:", str(e))
        raise


if __name__ == "__main__":
    run()
