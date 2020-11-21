from flask import Flask
from flask import abort
from flask import request, jsonify
from datetime import datetime, timedelta
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import json
import concurrent.futures

import data_structures

app = Flask(__name__)
CORS(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/welcomestats', methods=['GET'])
def api_welcome_stats():
    pass


@app.route('/api/blockdays', methods=['GET'])
def api_blockdays():
    date_string = request.args['date']
    return_data = {}

    try:
        date_object_from = datetime.strptime(date_string, '%Y-%m-%d')
    except Exception as ex:
        print(f"Datetime assignment failed {ex}")
    else:
        try:
            return_blockday = data_structures.BlockDay(date_object_from)
            return_data = json.dumps((return_blockday.data_retrieval(outline_only_request=True)))
        except Exception as ex:
            print('BlockDay Creation Failed', ex)
            abort(500)
        assert return_data
        return return_data


@app.route('/api/block', methods=['GET'])
def api_blocks():
    return_data = []
    block_hash = request.args['hash']
    try:
        database_lookup = data_structures.block_collection.find_one(block_hash)
        if database_lookup:
            return_data = database_lookup
    except Exception as ex:
        print(ex)
        abort(404)
    else:
        return return_data


@app.route('/api/transaction', methods=['GET'])
def api_transactions():
    transaction_hash = request.args['hash']
    return_data = []
    try:
        database_lookup = data_structures.transaction_collection.find_one(transaction_hash)
        if database_lookup:
            return_data = database_lookup
    except Exception as ex:
        print(ex)
        abort(404)
    else:
        return return_data


if __name__ == '__main__':
    app.run()
