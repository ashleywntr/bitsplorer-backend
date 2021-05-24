import gc
import json
import traceback
from concurrent.futures import as_completed
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask
from flask import abort
from flask import request
from flask_cors import CORS
from flask_csv import send_csv
from flask_executor import Executor
from requests_futures.sessions import FuturesSession

import data_structures
from data_structures import BlockDay, Address
from project_enum import RetrievalType

app = Flask(__name__)
executor = Executor(app)
CORS(app)

bitcoin_abuse_token = "wZe9GYRta5RN8s32QOKDmtmMBWkDXzi68ho5LXz4WmmBgstS3sOgRv44rnLZ"
currency_list = ['GBP', 'EUR', 'CNY']


@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello World!'


@app.route('/api', methods=['GET'])
def hello_api_world():
    return 'Hello api World!'


@app.route('/api/welcomestats', methods=['GET'])
def api_welcome_stats():
    pass


@app.route('/api/visualisation/sunburst', methods=['GET'])
def api_sunburst_visualisation():
    working_blockdays = []

    blockday_required_stats = []

    date_from = request.args['from']
    date_to = request.args['to']

    date_object_from = datetime.strptime(date_from, '%Y-%m-%d')
    date_object_to = datetime.strptime(date_to, '%Y-%m-%d')

    date_list = [date_object_from + timedelta(days=x) for x in range((date_object_to - date_object_from).days + 1)]

    for each in date_list:
        working_blockdays.append(BlockDay(each))

    for blockday in working_blockdays:
        data = blockday.data_retrieval(retrieval_type=RetrievalType.BLOCK_DATA_ONLY)
        block_list = []

        for block in blockday.instantiated_block_objects:
            # transaction_required_stats = [{'item': transaction.hash, 'value': transaction.value_outputs} for transaction in block.tx]
            block_required_stats = {'name': str(block.height), 'size': block.total_val_outputs_block}
            block_list.append(block_required_stats)

        blockday_required_stats.append({'name': data['_id'], 'children': block_list})

    json_outline = {
        'name': 'Blockday Graph',
        'children': blockday_required_stats
    }

    return json.dumps(json_outline)


@app.route('/api/csv/block', methods=['GET'])
def api_csv_block_list():
    date_string = request.args['date']
    block_data_for_csv = []

    date_object_from = datetime.strptime(date_string, '%Y-%m-%d')
    return_blockday = BlockDay(date_object_from)
    data_retrieval = return_blockday.data_retrieval(RetrievalType.OUTLINE_ONLY)
    block_list = data_retrieval['blocks']

    for x in block_list:
        result = data_structures.block_collection.find_one(x)
        result['hash'] = result.pop('_id')
        block_data_for_csv.append(result)

    block_data_for_csv.sort(key=lambda item: item.get("height"))
    assert len(block_list) == len(block_data_for_csv)

    fields_list = ["height", "average_fee_per_transaction", "average_num_inputs_per_transaction",
                   "average_num_outputs_per_transaction", "average_val_inputs_per_transaction",
                   "average_val_outputs_per_transaction",
                   "fee",
                   "main_chain",
                   "mrkl_root",
                   "n_tx",
                   "nonce",
                   "prev_block",
                   "size",
                   "time",
                   "total_num_inputs_block",
                   "total_num_outputs_block",
                   "total_val_fees_block",
                   "total_val_inputs_block",
                   "total_val_outputs_block", "hash"]

    return send_csv(block_data_for_csv, filename=f"Blocks {date_string}.csv", fields=fields_list,
                    writer_kwargs={"extrasaction": "ignore"})


@app.route('/api/csv/currency', methods=['GET'])
def api_csv_currency_report():
    fields_list = [
        'date', 'USD'
    ]
    fields_list.extend(currency_list)

    retrieval_date_from = request.args['date_from']
    retrieval_date_to = request.args['date_to']
    currency_json_data = currency_data_retriever(retrieval_date_from, retrieval_date_to)

    currency_csv_data = [dict(value, date=key) for key, value in currency_json_data.items()]
    return send_csv(currency_csv_data, filename=f"Currency Data {str(retrieval_date_from), str(retrieval_date_to)}.csv",
                    fields=fields_list)


@app.route('/api/csv/transactions', methods=['GET'])
def api_csv_transaction_list():
    search_hash = request.args['hash']
    transaction_data_for_csv = []

    block_result = data_structures.block_collection.find_one(search_hash)

    for tx in block_result['tx']:
        result = data_structures.transaction_collection.find_one(tx)

        result["number_of_inputs"] = result.pop('vin_sz')
        result["number_of_outputs"] = result.pop('vout_sz')
        result["transaction_hash"] = result.pop('_id')

        transaction_data_for_csv.append(result)

    transaction_data_for_csv.sort(key=lambda item: item.get("time"))
    assert len(block_result['tx']) == len(transaction_data_for_csv)

    fields_list = ["transaction_hash",
                   "block_height",
                   "number_of_inputs",
                   "number_of_outputs",
                   "coinbase_transaction",
                   "time",
                   "size",
                   "value_inputs",
                   "fee",
                   "value_outputs"]

    return send_csv(transaction_data_for_csv, filename=f"Transactions for Block {block_result['height']}.csv",
                    fields=fields_list, writer_kwargs={"extrasaction": "ignore"})


@app.route('/api/blockdays', methods=['GET'])
def api_blockdays():
    print(f"Blockday request: {request.args['date']}")
    gc.collect()
    date_string = request.args['date']
    return_data = {}

    try:
        date_object_from = datetime.strptime(date_string, '%Y-%m-%d')
    except Exception as ex:
        print(f"Datetime assignment failed {ex}")
    else:
        try:
            return_blockday = BlockDay(date_object_from)
            return_data = json.dumps((return_blockday.data_retrieval(RetrievalType.OUTLINE_ONLY)))
        except Exception as ex:
            print('BlockDay Creation Failed', ex)
            traceback.print_exc()
            abort(500)
        assert return_data
        return return_data


@app.route('/api/block', methods=['GET'])
def api_blocks():
    return_data = []
    block_hash = request.args['hash']
    try:
        database_lookup = data_structures.block_collection.find_one(block_hash)
        assert database_lookup
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

    return return_data


@app.route('/api/address', methods=['GET'])
def api_address():
    address_hash = request.args['hash']
    working_address = Address(address_hash)
    address_data = working_address.outline_retrieval()

    address_data['abuse_check'] = False
    address_data['abuse_count'] = 0

    try:
        abuse_check = requests.get(
            url=f"https://www.bitcoinabuse.com/api/reports/check?address={address_hash}&api_token={bitcoin_abuse_token}")
        abuse_check.raise_for_status()
    except:
        print('Unable to retrieve bitcoin abuse data')
    else:
        address_data['abuse_check'] = True
        address_data['abuse_count'] = abuse_check.json()['count']

    return address_data


@app.route('/api/address/transactions', methods=['GET'])
def api_address_transactions():
    address_hash = request.args['hash']
    working_address = Address(address_hash)
    return_data = working_address.outline_retrieval()
    working_address.tx_object_instantiation()
    return_data['txs'] = [x.attribute_return() for x in working_address.txs]
    return return_data


@app.route('/api/currency', methods=['GET'])
def api_currency_date():
    retrieval_date_from = request.args['date_from']
    retrieval_date_to = request.args['date_to']
    return currency_data_retriever(retrieval_date_from, retrieval_date_to)


def currency_data_retriever(retrieval_date_from, retrieval_date_to):
    coindesk_url = f'https://api.coindesk.com/v1/bpi/historical/close.json?start={retrieval_date_from}&end={retrieval_date_to}'
    print('Retrieving Currency Information from', coindesk_url)
    currency_request = requests.get(coindesk_url, headers=data_structures.default_headers)
    currency_request.raise_for_status()
    currency_data = currency_request.json()['bpi'].items()

    exchange_rate_data = exchange_data_retrieval(retrieval_date_from, retrieval_date_to)
    consolidated_data = {}
    for working_date, usd_value in currency_data:
        consolidated_data[working_date] = {
            'USD': usd_value,
        }
        exchange_date = working_date
        while exchange_date not in exchange_rate_data:
            exchange_date_object = datetime.fromisoformat(exchange_date).replace(tzinfo=timezone.utc)
            subtracted_date = (exchange_date_object - timedelta(days=1))
            exchange_date = subtracted_date.strftime('%Y-%m-%d')

        for currency in currency_list:
            if currency in exchange_rate_data[exchange_date]:
                consolidated_data[working_date][currency] = round(float(usd_value) * float(exchange_rate_data[exchange_date][currency]), 2)
    return consolidated_data


def exchange_data_retrieval(date_from, date_to):
    date_object_from = datetime.fromisoformat(date_from)
    date_object_to = datetime.fromisoformat(date_to)

    date_object_from.replace(tzinfo=timezone.utc)
    date_object_to.replace(tzinfo=timezone.utc)

    from_weekday = date_object_from.weekday()

    if from_weekday > 4:
        date_object_from = date_object_from - timedelta(days=from_weekday-4)
        date_from = date_object_from.strftime('%Y-%m-%d')
        print('Exchange retrieval updated to proceeding Friday')
    try:
        exchange_base_url = "https://freecurrencyapi.net/api/v1/rates"
        exchange_api_key="72741890-bc06-11eb-81e3-cd95bf08b9a9"
        exchange_rate_retrieval_url = f"{exchange_base_url}?base_currency=USD&date_from={date_from}&date_to={date_to}&apikey={exchange_api_key}"

        print(f"Retrieving Exchange Rate Data from {exchange_rate_retrieval_url}")
        exchange_headers = data_structures.default_headers
        exchange_headers['apikey'] = exchange_api_key
        exchange_request = requests.get(url=exchange_rate_retrieval_url, headers=exchange_headers, timeout=15)
        exchange_request.raise_for_status()
        exchange_rate_data = exchange_request.json()['data']

    except requests.exceptions.HTTPError as ex:
        if ex.response.status_code == 500:
            date_object_from = date_object_from - timedelta(days=1)
            date_from = date_object_from.strftime('%Y-%m-%d')
            return exchange_data_retrieval(date_from, date_to)
        if not ex.response.status_code == 429:
            print('Other HTTP error occurred', ex)
    except requests.exceptions.ReadTimeout as timeout:
        print("Request read timeout", timeout)
    except requests.exceptions.ConnectionError as connection_error:
        print("Connection Error Occurred", connection_error)
    else:
        return exchange_rate_data


if __name__ == '__main__':
    app.run()
