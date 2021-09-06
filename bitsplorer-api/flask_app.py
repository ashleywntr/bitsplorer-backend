import gc
import json
from datetime import datetime, timedelta

import requests
from flask import Flask
from flask import abort
from flask import request
from flask_cors import CORS
from flask_csv import send_csv
from flask_executor import Executor

from bson.objectid import ObjectId

import currency_logic
import data_structures
from data_structures import BlockDay, Address, AbuseReport
from project_enum import RetrievalType

app = Flask(__name__)
executor = Executor(app)
CORS(app)

bitcoin_abuse_token = "wZe9GYRta5RN8s32QOKDmtmMBWkDXzi68ho5LXz4WmmBgstS3sOgRv44rnLZ"
currency_list = ['GBP', 'EUR', 'CNY']


@app.route('/', methods=['GET'])
def hello_world():
    page_content = ""
    page_content += "<div><h1> Bitsplorer Backend </h1></div>"
    page_content += "\n<div><h3> Please choose some example functions</h3>"

    page_content += "\n<h4> Block Explorer Examples </h4>"
    page_content += '\n<a href="../api/blockdays?date=2020-07-31"> BlockDay Request </a><br>'
    page_content += '\n<a href="../api/csv/block?date=2020-07-31"> BlockDay CSV Request </a><br>'
    page_content += '\n<a href="../api/block?hash=000000000000000000042be75dce3cb116c9720801a9208bcb556c6f8604b059"> Block Request </a><br>'
    page_content += '\n<a href="../api/transaction?hash=f205316b3c722247c6bad75e71a9e2e14d1f7329c042ff7797593cd67f982686"> Transaction Request </a>'

    page_content += "\n<h4> Address Explorer Examples </h4>"
    page_content += '\n<a href="../api/address?hash=172fgNkRaoDmAqpbkct5TUYBr1PodCF4pZ"> Address Request </a>'

    page_content += "\n<h4> Currency Examples </h4>"
    page_content += '\n<a href="../api/currency?date_from=2020-07-19&date_to=2020-07-31"> Currency JSON Request </a><br>'
    page_content += '\n<a href="../api/csv/currency?date_from=2020-05-07&date_to=2020-05-11"> Currency CSV Request </a>'

    page_content += '\n<h4> POST Forms </h4>'
    page_content += '\n<a href="../api/post/test/flagging/address"> Address Flagging Form</a>'
    page_content += "\n</div>"
    return page_content


@app.route('/api', methods=['GET'])
def hello_api_world():
    return 'Hello World!'

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
    currency_json_data = currency_logic.currency_data_retriever(retrieval_date_from, retrieval_date_to)

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

    try:
        date_object = datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        abort(400)
    else:
        return_blockday = BlockDay(date_object)
        return_data = json.dumps((return_blockday.data_retrieval(RetrievalType.OUTLINE_ONLY)))
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

    database_lookup = data_structures.transaction_collection.find_one(transaction_hash)
    if database_lookup:
        tx_item = data_structures.Transaction(database_lookup, True)
        return_data = tx_item.attribute_return()

    return return_data


@app.route('/api/address', methods=['GET'])
def api_address():
    address_hash = request.args['hash']
    working_address = Address(address_hash)
    address_data = working_address.outline_retrieval()

    address_data['abuse_check'] = False
    address_data['abuse_count'] = 0

    abuse_url = f"https://www.bitcoinabuse.com/api/reports/check?address={address_hash}&api_token={bitcoin_abuse_token}"

    try:
        print(f"Retrieving abuse data from {abuse_url}")
        abuse_check = requests.get(
            url=abuse_url)
        abuse_check.raise_for_status()
    except Exception:
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
    if 'offset' in request.args:
        offset = int(request.args['offset'])
        return_data['txs'] = working_address.blockchain_info_api_tx_retrieval(offset)
    else:
        working_address.tx_object_instantiation()
        return_data['txs'] = [x.attribute_return() for x in working_address.txs]
    return return_data


@app.route('/api/currency', methods=['GET'])
def api_currency_date():
    retrieval_date_from = request.args['date_from']
    retrieval_date_to = request.args['date_to']
    return currency_logic.currency_data_retriever(retrieval_date_from, retrieval_date_to)


@app.route('/api/abuse', methods=['GET'])
def api_abuse_retrieve_all():
    abuse_cursor = data_structures.abuse_collection.find().sort('date')

    return_data = []

    for entry in abuse_cursor:
        print(entry)
        entry['_id'] = str(entry['_id'])
        entry['date'] = datetime.strftime(entry['date'], '%Y-%m-%d')
        return_data.append(entry)

    return json.dumps(return_data)


# POST
@app.route('/api/post/flagging/remove', methods=['POST'])
def api_post_remove_abuse():
    data = request.form
    assert data['_id']

    data_structures.abuse_collection.find_one_and_delete({'_id': ObjectId(data['_id'])})

    body = f"Removed {data['_id']} from database"
    return body


@app.route('/api/post/test/flagging/address', methods=['GET'])
def api_post_address_test():
    body = ""
    body += "<h1>Address Post Test</h1>"
    body += '<form action="/api/post/flagging/address" method="post">'
    body += '<label for="address_field">Address</label>'
    body += '<input type="text" id="address_field" name="address"/><br>'
    body += '<label for="source_field">Source</label>'
    body += '<input type="text" id="source_field" name="source"/><br>'
    body += '<label for="notes_field">Notes</label>'
    body += '<input type="text" id="notes_field" name="notes"/><br>'
    body += '<input type="date" id="date_field" name="date"/><br>'
    body += '<input type="submit" />'
    body += '</form>'
    return body


@app.route('/api/post/flagging/address', methods=['POST'])
def api_post_address_flag():
    data = request.form
    print('Received Data')
    for key, value in data.items():
        print(key, value)

    abuse_object = AbuseReport(data)

    body = ""
    body += 'Abuse Data Received: <br>'
    for key, value in abuse_object.attribute_exporter().items():
        body += f'{key}: {value} <br>'
    body += '<a href="../test/flagging/address">Return to Form</a>'
    return body


if __name__ == '__main__':
    app.run()
