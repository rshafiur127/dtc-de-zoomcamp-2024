import pandas as pd
import json
import datetime as dt
import time
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

t0 = time.time()

topic_name = 'green-trips'

# Read only this columns
usecols = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "trip_distance"]

# Read data with subset of columns
df_green = pd.read_csv("green_tripdata_2019-10.csv", usecols=usecols)


for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)
    print(row_dict)


producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
