
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

def main():
    # Create protobuf serializer


    # Create a Kafka producer
    feedEntity = gtfs_realtime_pb2.FeedEntity()
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: feedEntity.SerializeToString(v).encode('utf-8')
    )

    # Pull data from API
    url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace'

    try: 
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: RequestException from API call: {e}")

    # Parsing feed into individual messages
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Send messages to kafka
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            producer.send('updates-data', value=entity)
        if entity.HasField('vehicle'):
            producer.send('vehicle-data', value=entity)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()