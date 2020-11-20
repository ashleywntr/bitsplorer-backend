import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from datetime import datetime
import traceback
import json

import requests
from pymongo import MongoClient
from requests_futures.sessions import FuturesSession
from rich.console import Console

from flask import jsonify

default_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.74 Safari/537.36 Edg/79.0.309.43'}

console = Console()

db_address = "mongodb://wntrdesktop:27017/"

database_client = MongoClient(db_address)
db_slice = 'pychain-dev'

database = database_client[db_slice]
transaction_collection = database["Transactions"]
block_collection = database["Blocks"]
blockday_collection = database["BlockDays"]
address_collection = database["Addresses"]

BlockDays = []
Address_Data: dict = {}

automatic_database_export = True


def single_getter(url, session, headers):
    print("Single Getter retrieving for", url)
    try:
        session_data_json = session.get(url=url, headers=headers)
        session_data_json.raise_for_status()
        session_data_dict = session_data_json.json()

    except Exception as ex:
        print("Single Request Fail", ex)
        raise Exception("Single Request Failed")
    else:
        return session_data_dict


def mongo_db_getter(client: MongoClient, search_string: str, transactions_required=True):
    result = client.find_one(search_string)
    if result:
        try:
            block_result = Block(result, retrieved_from_db=True, transactions_required=transactions_required)
        except Exception as ex:
            print("Unable to assign result to block", ex)
        else:
            print('Returning Complete Block from DB')
            return block_result
    else:
        raise TypeError('Cant assign blank request to block')


def mongo_db_getter_transaction(client: MongoClient, search_string: str):

    result = client.find_one(search_string)
    assert result
    if result:
        try:
            transaction_result = Transaction(result, retrieved_from_db=True)
        except Exception as ex:
            print("Unable to assign result to transaction", ex)
        else:
            return transaction_result
    else:
        raise TypeError('Transaction Not Found. Re-Instantiating Block')


class BlockDay:
    def __init__(self, timestamp: datetime):
        # Properties
        self.timestamp = timestamp
        self._id = timestamp.strftime('%d%m%Y')

        self.block_outline_dict: dict = {}
        self.block_instantiated_object_dict: dict = {}
        self.failed_retrievals = ['Initialise fake value as there is no do before while loop']

        self.outline_in_db = False
        self.db_block_list = []
        # Statistics
        self.total_num_blocks = 0
        self.total_num_tx: int = 0
        self.total_num_inputs: int = 0
        self.total_num_outputs: int = 0
        self.total_val_inputs: int = 0
        self.total_val_fees_block: int = 0
        self.total_val_outputs: int = 0

        self.avg_num_inputs = 0
        self.avg_num_outputs = 0
        self.avg_val_inputs = 0
        self.avg_val_outputs = 0

    def data_retrieval(self, outline_only=False):
        database_lookup = blockday_collection.find_one(self._id)
        try:  # Assign values from API import
            self.block_outline_dict = {block: {'height': 0, 'time': 0, 'hash': block} for block in
                                       database_lookup['blocks']}

            self.total_num_blocks = database_lookup['total_num_blocks']
            self.total_num_tx: int = database_lookup['total_num_tx']
            self.total_num_inputs: int = database_lookup['total_num_inputs']
            self.total_num_outputs: int = database_lookup['total_num_outputs']
            self.total_val_inputs: int = database_lookup['total_val_inputs']
            self.total_val_fees_block: int = database_lookup['total_val_fees_block']
            self.total_val_outputs: int = database_lookup['total_val_outputs']

            self.db_block_list = database_lookup['blocks']
            self.avg_num_inputs = database_lookup['avg_num_inputs']
            self.avg_num_outputs = database_lookup['avg_num_outputs']

            self.avg_val_inputs = database_lookup['avg_val_inputs']
            self.avg_val_outputs = database_lookup['total_val_outputs']
            self.outline_in_db = True

        except Exception as EX:
            print("Blockday not in DB", EX)
            self.blockday_outline_api_retrieval()
        finally:
            if not self.outline_in_db or not outline_only:
                print(f"Retrieving Blocks for BlockDay {self._id}")
                self.retrieve_blocks()
            return True

    def blockday_outline_api_retrieval(self):
        timestamp_corrected = self.timestamp  # + timedelta(days=1)
        timestamp_in_milliseconds = timestamp_corrected.timestamp() * 1000
        timestamp_as_string = str(timestamp_in_milliseconds)[:-2]
        block_importer_url = f'https://blockchain.info/blocks/{timestamp_as_string}?format=json'

        with requests.session() as block_outline_import_session:
            request_itr = 0
            block_data_dict = {}
            while request_itr < 5:
                request_itr += 1
                try:
                    block_data_dict = single_getter(url=block_importer_url, session=block_outline_import_session,
                                                    headers=default_headers)
                except Exception as ex:
                    print("Failure to instantiate block data dict. Will retry", ex)
                else:
                    break
                if request_itr == 5:
                    raise Exception('Failed to retrieve block data dict')
        try:
            self.block_outline_dict = {block['hash']: block for block in block_data_dict['blocks']}
        except Exception as ex:
            print(f'Block Import Dict assignment Failed {ex}')

    def retrieve_blocks(self, transactions_required=True):
        futures = []
        working_list = []

        with ThreadPoolExecutor(max_workers=500) as executor:
            for something, x in self.block_outline_dict.items():
                try:
                    mongo_client = MongoClient(db_address)[db_slice]['Blocks']
                    futures.append(executor.submit(mongo_db_getter(mongo_client, x['hash'], transactions_required=False)))
                except Exception as ex:
                    print("Block or Transaction Data missing from DB", ex)
                    working_list.append(x['hash'])

            for future in as_completed(futures):
                try:
                    result = future.result()
                    block_object = result
                    self.block_instantiated_object_dict[block_object.hash] = block_object
                    print(f"Block {block_object.height} retrieved from DB")
                except Exception as ex:
                    print("Incorrect object returned from threaded DB function")
                    working_list.append(x['hash'])

        if working_list:
            self.retrieve_blocks_from_api(working_list)
        else:
            print("All required values were found in Database")
        self.statistics_generation()

        print(f"Total number of blocks is {self.total_num_blocks}")
        print(f"Length of block outline dict is {len(self.block_outline_dict)}")
        print("Attempting Assertion")

        assert self.total_num_blocks == len(self.block_outline_dict)

        if automatic_database_export:
            try:
                self.db_attribute_exporter()
            except Exception as ex:
                print('Unable to export Blockday to DB')

    def retrieve_blocks_from_api(self, working_list):
        loop_count = 0
        while working_list:
            print(f'{len(working_list)} entries on {self._id} working list')
            with FuturesSession(max_workers=40) as session:
                futures = []
                for x in working_list:
                    block_hash = f'{x}'
                    block_api_single_block_url = f'https://blockchain.info/rawblock/{block_hash}'
                    futures.append(session.get(url=block_api_single_block_url, headers=default_headers, timeout=15))

            for future in as_completed(futures):
                try:
                    result = future.result()
                    result.raise_for_status()
                    block_dict = result.json()

                    block_object = Block(block_dict)

                    self.block_instantiated_object_dict[block_object.hash] = block_object

                    working_list.remove(block_object.hash)
                    print(f'{len(working_list)} entries on {self._id} working list')


                except requests.exceptions.HTTPError as ex:
                    if ex.response.status_code == 429:
                        console.print('API Request Limit Exceeded')
                    else:
                        console.print('Other HTTP error occurred')
                except Exception as ex:
                    console.print("Other error occurred", ex)

        console.print(f'Failed {len(working_list)} retrievals')
        loop_count += 1
        if loop_count == 15:
            raise Exception("Failed to retrieve all values on working list")

    def statistics_generation(self):
        self.total_num_blocks = len(self.block_instantiated_object_dict)
        for bl_hash, block in self.block_instantiated_object_dict.items():
            self.total_num_tx += block.n_tx
            self.total_num_inputs += block.total_num_inputs_block
            self.total_num_outputs += block.total_num_outputs_block
            self.total_val_inputs += block.total_val_inputs_block
            self.total_val_fees_block += block.total_val_fees_block
            self.total_val_outputs += block.total_val_outputs_block

            self.avg_num_inputs = self.total_num_inputs / self.total_num_tx
            self.avg_num_outputs = self.total_num_outputs / self.total_num_tx

            self.avg_val_inputs = self.total_val_inputs / self.total_num_tx
            self.avg_val_outputs = self.total_val_outputs / self.total_num_tx

    def db_attribute_exporter(self, only_return=False):
        export_attributes = {}
        excluded_keys = {'block_instantiated_object_dict', 'block_outline_dict', 'failed_retrievals',
                         'data_instantiated', 'cached_import', 'outline_in_db', 'db_block_list', 'timestamp'}
        try:
            for attribute, value in vars(self).items():
                if attribute not in excluded_keys:
                    export_attributes[attribute] = value
        except Exception as ex:
            print("Automatic attribute generation failed", ex)

        try:
            export_attributes['blocks'] = [x for x in self.block_instantiated_object_dict]
            assert len(export_attributes['blocks']) == self.total_num_blocks
        except Exception as ex:
            print("Number of Instantiated Blocks does not match Total", ex)
            try:
                export_attributes['blocks'] = self.db_block_list
                print("Exported block list from database value")
            except Exception as ex:
                print("No block information found for Blockday")

        if not only_return:
            try:
                blockday_collection.insert_one(export_attributes)
            except Exception as ex:
                print("BlockDay Export Failed", ex)
        else:
            return export_attributes


class Block:
    def __init__(self, block_attr_dict, retrieved_from_db=False, transactions_required=True):
        # Changing Schema Vars
        self.hash = ""
        self._id = ""
        self.tx = []

        # Non Changing Schema Vars

        self.time: int = block_attr_dict['time']  # Timestamp of the block (unix format)
        self.fee: int = block_attr_dict['fee']
        self.nonce: int = block_attr_dict['nonce']
        self.n_tx: int = block_attr_dict['n_tx']
        self.size: int = block_attr_dict['size']
        self.main_chain: bool = block_attr_dict['main_chain']
        self.height: int = block_attr_dict['height']
        self.prev_block: str = block_attr_dict['prev_block']
        self.mrkl_root: str = block_attr_dict['mrkl_root']

        # Calculated Attributes
        self.total_num_inputs_block: int = 0
        self.total_num_outputs_block: int = 0

        self.total_val_inputs_block: int = 0
        self.total_val_fees_block: int = 0
        self.total_val_outputs_block: int = 0

        self.average_val_inputs_per_transaction = 0
        self.average_fee_per_transaction = 0
        self.average_val_outputs_per_transaction = 0

        self.average_num_inputs_per_transaction = 0
        self.average_num_outputs_per_transaction = 0

        if not retrieved_from_db:
            self.hash: str = block_attr_dict['hash']
            self._id = self.hash
            for tx in block_attr_dict['tx']:
                tx['block_hash'] = self.hash
            self.tx = [Transaction(x) for x in block_attr_dict['tx']]
            self.statistics_generation()

            if automatic_database_export:
                self.db_attribute_exporter()
        else:
            self.hash = block_attr_dict['_id']
            if transactions_required:
                self.retrieve_transactions_from_db(block_attr_dict['tx'])

            self.total_num_inputs_block: int = block_attr_dict['total_num_inputs_block']
            self.total_num_outputs_block: int = block_attr_dict['total_num_outputs_block']

            self.total_val_inputs_block: int = block_attr_dict['total_val_inputs_block']
            self.total_val_fees_block: int = block_attr_dict['total_val_fees_block']
            self.total_val_outputs_block: int = block_attr_dict['total_val_outputs_block']

            self.average_val_inputs_per_transaction = block_attr_dict['average_val_inputs_per_transaction']
            self.average_fee_per_transaction = block_attr_dict['average_fee_per_transaction']
            self.average_val_outputs_per_transaction = block_attr_dict['average_val_outputs_per_transaction']

            self.average_num_inputs_per_transaction = block_attr_dict['average_num_inputs_per_transaction']
            self.average_num_outputs_per_transaction = block_attr_dict['average_num_outputs_per_transaction']

    def retrieve_transactions_from_db(self, tx_list):
        futures_list = []
        with ThreadPoolExecutor(max_workers=500) as executor:
            for tx in tx_list:
                client = MongoClient(db_address)[db_slice]["Transactions"]
                futures_list.append(executor.submit(mongo_db_getter_transaction(client=client, search_string=tx, block_hash=self.hash)))
            try:
                for future in as_completed(futures_list):
                    self.tx.append(future.result())
            except Exception as ex:
                raise Exception("Transaction missing from DB")

    def statistics_generation(self):

        for transaction in self.tx:
            self.total_num_inputs_block += transaction.vin_sz
            self.total_num_outputs_block += transaction.vout_sz

            self.total_val_inputs_block += transaction.value_inputs
            self.total_val_fees_block += transaction.fee
            self.total_val_outputs_block += transaction.value_outputs

        self.average_val_inputs_per_transaction = self.total_val_inputs_block / self.n_tx
        self.average_fee_per_transaction = self.total_val_fees_block / self.n_tx
        self.average_val_outputs_per_transaction = self.total_val_outputs_block / self.n_tx

        self.average_num_inputs_per_transaction = self.total_num_inputs_block / self.n_tx
        self.average_num_outputs_per_transaction = self.total_num_outputs_block / self.n_tx

    def db_attribute_exporter(self):
        export_attributes = {}
        excluded_keys = {'tx', 'block_attribute_table', 'hash', 'cached_import', 'data_instantiated'}
        try:
            for attribute, value in vars(self).items():
                if attribute not in excluded_keys:
                    export_attributes[attribute] = value
        except Exception as ex:
            print("Automatic attribute generation failed", ex)
        try:
            export_attributes['tx'] = [x.hash for x in self.tx]
        except Exception as ex:
            print("Assignment of inputs and exports failed", ex)

        try:
            block_collection.insert_one(export_attributes)
            print(f"Block {self.height} Successfully Exported")
        except Exception as ex:
            print("Block Export Failed", ex)

    def __call__(self):
        return self


class Transaction:
    def __init__(self, transaction_attr_dict: dict, retrieved_from_db=False):

        # Changing Schema Vars
        self.coinbase_transaction: bool
        self.hash = ""  # Unique hash associated with the transaction
        self._id = ""

        # Non Changing Schema Vars
        self.vin_sz: int = transaction_attr_dict['vin_sz']  # Number of inputs into the transaction
        self.vout_sz: int = transaction_attr_dict['vout_sz']  # number of outputs from the transaction
        self.size: int = transaction_attr_dict['size']
        self.time: int = transaction_attr_dict['time']
        self.block_height: int = transaction_attr_dict['block_height']
        self.block_hash = transaction_attr_dict['block_hash']
        self.fee: int = transaction_attr_dict['fee']
        self.double_spend: bool = transaction_attr_dict['double_spend']
        self.inputs: list = transaction_attr_dict['inputs']
        self.out: list = transaction_attr_dict['out']

        self.value_inputs = 0
        self.value_outputs = 0

        # Statistics
        if not retrieved_from_db:
            self.statistics_generation()
            self.hash = transaction_attr_dict['hash']
            self._id = self.hash

            if self.value_inputs == 0:
                self.coinbase_transaction = True
            else:
                self.coinbase_transaction = False

            if automatic_database_export:
                self.attribute_exporter()
        else:
            self.hash = transaction_attr_dict['_id']
            self.id = transaction_attr_dict['_id']
            self.value_inputs = transaction_attr_dict['value_inputs']
            self.value_outputs = transaction_attr_dict['value_outputs']
            self.coinbase_transaction = transaction_attr_dict['coinbase_transaction']

    def statistics_generation(self):
        for tr_input in self.inputs:
            try:
                self.value_inputs += tr_input['prev_out']['value']
            except KeyError:
                pass
        for tr_output in self.out:
            self.value_outputs += tr_output['value']

    def attribute_exporter(self):
        export_attributes = {}
        excluded_keys = {'hash', 'cached_import'}
        try:
            for attribute, value in vars(self).items():
                if attribute not in excluded_keys:
                    export_attributes[attribute] = value
        except Exception as ex:
            print("Automatic attribute generation failed", ex)
        try:
            transaction_collection.insert_one(export_attributes)
        except Exception as ex:
            pass

    def __call__(self, *args, **kwargs):
        return self
