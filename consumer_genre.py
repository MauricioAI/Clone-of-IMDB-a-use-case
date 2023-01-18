from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json


es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])
consumer = KafkaConsumer('imdb_genre', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for genre in consumer:
    index_genres = es.index(index='genres', document=genre.value, timeout=30)

