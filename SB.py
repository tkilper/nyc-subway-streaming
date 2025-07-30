
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2

# check source data

# A, C, E, Sr line data
try:
    url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace'
    response = requests.get(url)
    response.raise_for_status() # Raise an exception for bad status codes

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            print(entity.trip_update)

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")