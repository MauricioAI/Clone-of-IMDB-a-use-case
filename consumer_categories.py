from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json


es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])
consumer = KafkaConsumer('imdb_categories', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for categorie in consumer:
    index_categories = es.index(index='categories', document=categorie.value, timeout=30)

