
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092', # change
    )

    url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace'

    try: 
        response = requests.get(url)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                producer.send('updates-data', value=entity)
            if entity.HasField('vehicle'):
                producer.send('vehicle-data', value=entity)
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()