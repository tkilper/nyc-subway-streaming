
import time
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

POLL_INTERVAL = 30
RETRY_DELAY = 5
URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace'


def fetch_feed():
    response = requests.get(URL)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def poll(producer):
    try:
        feed = fetch_feed()
    except requests.exceptions.RequestException as e:
        print(f"Error: RequestException from API call: {e}. Retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
        try:
            feed = fetch_feed()
        except requests.exceptions.RequestException as e:
            print(f"Error: Retry failed: {e}. Skipping poll.")
            return

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            producer.send('updates-data', value=entity)
        if entity.HasField('vehicle'):
            producer.send('vehicle-data', value=entity)

    producer.flush()


def main():
    print("Creating Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: v.SerializeToString()
    )

    print(f"Starting MTA producer. Polling every {POLL_INTERVAL}s.")
    try:
        while True:
            poll(producer)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Shutting down producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()