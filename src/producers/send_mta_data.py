
import time
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

POLL_INTERVAL = 30
RETRY_DELAY = 5

# MTA GTFS-realtime feed endpoints (no API key required). Together these cover
# every NYC subway line. The feed id is the trailing path segment, URL-encoded
# (`%2F` == `/`).
FEED_BASE = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/'
FEED_URLS = [
    FEED_BASE + 'nyct%2Fgtfs',       # 1 2 3 4 5 6 7 and the 42 St Shuttle (S)
    FEED_BASE + 'nyct%2Fgtfs-ace',   # A C E and the Rockaway Park Shuttle (H)
    FEED_BASE + 'nyct%2Fgtfs-bdfm',  # B D F M
    FEED_BASE + 'nyct%2Fgtfs-g',     # G
    FEED_BASE + 'nyct%2Fgtfs-jz',    # J Z
    FEED_BASE + 'nyct%2Fgtfs-nqrw',  # N Q R W
    FEED_BASE + 'nyct%2Fgtfs-l',     # L
    FEED_BASE + 'nyct%2Fgtfs-si',    # Staten Island Railway (SIR)
]


def fetch_feed(url):
    response = requests.get(url)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def fetch_feed_with_retry(url):
    """Fetch one feed, retrying once after RETRY_DELAY on a request error.

    Returns the FeedMessage, or None if both attempts fail so the caller can
    skip that feed without aborting the whole poll.
    """
    try:
        return fetch_feed(url)
    except requests.exceptions.RequestException as e:
        print(f"Error: RequestException from {url}: {e}. Retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
        try:
            return fetch_feed(url)
        except requests.exceptions.RequestException as e:
            print(f"Error: Retry failed for {url}: {e}. Skipping feed.")
            return None


def publish_feed(producer, feed):
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            producer.send('updates-data', value=entity)
        if entity.HasField('vehicle'):
            producer.send('vehicle-data', value=entity)


def poll(producer):
    published_any = False
    for url in FEED_URLS:
        feed = fetch_feed_with_retry(url)
        if feed is None:
            continue
        publish_feed(producer, feed)
        published_any = True

    if published_any:
        producer.flush()


def main():
    print("Creating Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: v.SerializeToString()
    )

    print(f"Starting MTA producer. Polling {len(FEED_URLS)} feeds every {POLL_INTERVAL}s.")
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
