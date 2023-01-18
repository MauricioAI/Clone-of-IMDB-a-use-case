from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])
consumer = KafkaConsumer('imdb_movies', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for movie in consumer:
    index_movies = es.index(index='movies', document=movie.value, timeout=30)

