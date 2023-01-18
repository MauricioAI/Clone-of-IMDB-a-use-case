from elasticsearch import Elasticsearch
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json


app = Flask(__name__)
es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])

producer = KafkaProducer(bootstrap_servers='localhost:9092')


@app.route('/')
def list_movies():
    query_all_movies = {'query': {'match_all': {}}}

    result = es.search(index='movies', query=query_all_movies)

    return jsonify(result)


@app.route('/search_movie_creator')
def search_movie_creator():
    query_movie = {'query': {'match': {'creator': request.args.get('creator') }}}

    result = es.search(index='movie', query=query_movie)

    return jsonify(result)

@app.route('/query_movie/<int:id_movie>')
def search_movie(id_movie):
    result = es.get(index='movies', id=id_movie)

    return jsonify(result)


@app.route('/new_movie', methods=['POST'])
def add_movie():
    json_payload = json.dumps(request.get_json())
    body_movies = str.encode(json_payload)
    producer.send('imdb_movies', body_movies)
    producer.flush()
    return jsonify({"result": "A new movie will be registered in a short time!!"})


@app.route('/new_categorie', methods=['POST'])
def add_categorie():
    json_payload = json.dumps(request.get_json())
    body_categories = str.encode(json_payload)
    producer.send('imdb_categories', body_categories)
    producer.flush()
    return jsonify({"result": "A new categorie will be registered in a short time!!"})


@app.route('/new_genre', methods=['POST'])
def add_genre():
    json_payload = json.dumps(request.get_json())
    body_genre = str.encode(json_payload)
    producer.send('imdb_genre', body_genre)
    producer.flush()
    return jsonify({"result": "A new genre will be registered in a short time!!"})



@app.route('/update_movie/<int:id_movie>', methods=['PUT'])
def update_movie(id_movie):
    update_body = {'doc': request.get_json()}

    result = es.update(index='movies', id=id_movie, doc=update_body)

    return jsonify(result)


@app.route('/remove_movie', methods=['DELETE'])
def remove_episode():
    id_movie = request.args.get('id')
    result = es.delete(index='movies', id=id_movie, timeout=30)

    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
