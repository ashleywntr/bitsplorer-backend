from concurrent.futures import as_completed
from copy import deepcopy
from datetime import datetime, timezone
from math import floor
from time import sleep

import pymongo.errors
import requests
from pymongo import MongoClient
from pymongo import errors
from requests_futures.sessions import FuturesSession

from project_enum import RetrievalType

default_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.74 Safari/537.36 Edg/79.0.309.43'}

SATOSHI_MULTIPLIER = 10 ** 8

db_address = "mongodb://192.168.1.249:27017/"

database_client = MongoClient(db_address)
db_slice = 'pychain-dev'  # Can be altered to correspond to / create new databases within Mongo
database = database_client[db_slice]
transaction_collection = database["Transactions"]
block_collection = database["Blocks"]
blockday_collection = database["BlockDays"]
address_collection = database["Addresses"]
abuse_collection = database["Abuse"]

automatic_database_export = True  # Default value for 'attribute exporter'. Disabling will prevent export to DB


class BlockDay:
    def __init__(self, timestamp: datetime):
        # Properties
        self.timestamp = timestamp.replace(tzinfo=timezone.utc)
        self._id = timestamp.strftime('%Y-%m-%d')

        self.block_outline_list = []
        self.instantiated_block_objects = []

        self.imported_from_db = False
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

    def data_retrieval(self, retrieval_type):
        try:
            database_lookup = blockday_collection.find_one(self._id)
            assert database_lookup  # Assert that Blockday has been found in DB
        except AssertionError:  # Catch assertion failure upon successful connection
            print("Assertion Error: BlockDay not in database")
            self.blockday_initial_api_retrieval()
            self.retrieve_blocks(
                import_transactions=False)  # Only block level data required to instantiate BlockDay
            return self.attribute_exporter(only_return=True)  # Return attributes of class without exporting to DB
        else:
            self.block_outline_list = [{'height': 0, 'time': 0, 'hash': block} for block in database_lookup['blocks']]
            # Assign data to class fields
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
            self.avg_val_outputs = database_lookup['avg_val_outputs']
            self.imported_from_db = True

            if retrieval_type == RetrievalType.FULL_RETRIEVAL:
                print('Retrieving All BlockDay Data and adding to memory')
                self.retrieve_blocks(import_transactions=True)
            elif retrieval_type == RetrievalType.BLOCK_DATA_ONLY:
                print(f"Retrieving Blocks for BlockDay  {self._id} without transactions")
                self.retrieve_blocks(import_transactions=False)
            elif retrieval_type == RetrievalType.OUTLINE_ONLY:
                print("Returning Outline data of Instantiated block.")

            return self.attribute_exporter(only_return=True)

    def blockday_initial_api_retrieval(self):  # Retrieve the outline of a day's worth of information from the API
        print(f'Retrieving BlockDay {self._id} from API')
        # Convert supplied timestamp into milliseconds for API retrieval
        timestamp_in_milliseconds = self.timestamp.timestamp() * 1000
        timestamp_as_string = str(timestamp_in_milliseconds)[:-2]
        blockday_outline_importer_url = f'https://blockchain.info/blocks/{timestamp_as_string}?format=json'
        print(f'Url for {self._id}: {blockday_outline_importer_url}')

        with requests.session() as block_outline_import_session:
            request_itr = 0
            blockday_data_dict = {}
            while True:
                request_itr += 1
                try:
                    session_data_json = block_outline_import_session.get(url=blockday_outline_importer_url,
                                                                         headers=default_headers)
                    session_data_json.raise_for_status()
                    blockday_data_dict = session_data_json.json()
                except Exception as ex:
                    print("Failure to instantiate block data dict. Will retry", ex)
                else:
                    break
                if request_itr == 50:
                    raise Exception('250 retries attempted. Failed to retrieve block data dict')

        assert blockday_data_dict  # Assert that the data is present
        self.block_outline_list = blockday_data_dict

    def retrieve_blocks(self, import_transactions=True):  # Retrieve Blocks - determine whether from API or DB
        working_list = []  # Will only be written to if blocks are missing from DB
        for x in self.block_outline_list:
            try:
                result = block_collection.find_one(x['hash'])
                assert result
            except AssertionError:
                working_list.append(x['hash'])
            else:
                block_object = Block(result, retrieved_from_db=True, transactions_required=import_transactions)
                self.instantiated_block_objects.append(block_object)
                print(f"Block {block_object.height} retrieved from DB")

        if working_list:
            self.retrieve_blocks_from_api(working_list)
        else:
            print("All required values were retrieved from DB")

        assert len(self.instantiated_block_objects) == len(self.block_outline_list)  # Check whether the number of
        # instantiated block objects matches the length of the retrieval list.

        if not self.imported_from_db:
            self.statistics_generation()

        if automatic_database_export:
            try:
                self.attribute_exporter()
            except Exception as ex:
                print('Unable to export Blockday to DB', {ex})

    def retrieve_blocks_from_api(self, working_list):  # Begin retrieval of Block hashes present in working_list
        loop_count = 0
        print(f'Beginning retrieval of {len(working_list)} entries on {self._id} working list')
        while working_list:
            with FuturesSession(max_workers=100) as session:
                futures = []
                for block_hash in working_list:
                    block_api_single_block_url = f'https://blockchain.info/rawblock/{block_hash}'
                    try:
                        futures.append(session.get(url=block_api_single_block_url, headers=default_headers, timeout=15))
                    except Exception as ex:
                        print('Exception in Block Retrieval', ex)
                    except requests.exceptions.HTTPError as ex:  # Ignore API overload related errors
                        if not ex.response.status_code == 429:
                            print('Other HTTP error occurred in request', ex)

                for future in as_completed(futures):  # Process responses as soon as they are retrieved
                    try:
                        result = future.result()  # Parse future object back to Requests' response object
                        result.raise_for_status()  # Raise exception if status not 200 normal
                        parsed_result = result.json()  # Interpret response data as JSON
                        self.instantiated_block_objects.append(Block(parsed_result))  # Instantiate Block object and
                        # append to self block list
                    except requests.exceptions.HTTPError as ex:
                        if not ex.response.status_code == 429:
                            print('Other HTTP error occurred in response', ex)
                    except requests.exceptions.ReadTimeout as timeout:
                        print("Response read timeout", timeout)
                    except requests.exceptions.ConnectionError as connection_error:
                        print("Connection Error Occurred", connection_error)
                    else:
                        working_list.remove(parsed_result['hash'])  # Remove entry from list if present in response
                        print(f'{len(working_list)} entries remaining on {self._id} working list')

            print(f'Failed {len(working_list)} retrievals')
            loop_count += 1
            if loop_count == 30:
                raise Exception(f"Failed to retrieve all values on working list after {loop_count} tries")

    def statistics_generation(self):
        self.total_num_blocks = len(self.instantiated_block_objects)
        for block in self.instantiated_block_objects:
            self.total_num_tx += block.n_tx
            self.total_num_inputs += block.total_num_inputs_block
            self.total_num_outputs += block.total_num_outputs_block
            self.total_val_inputs += block.total_val_inputs_block
            self.total_val_fees_block += block.total_val_fees_block
            self.total_val_outputs += block.total_val_outputs_block

            self.avg_num_inputs = self.total_num_inputs / self.total_num_tx
            self.avg_num_outputs = self.total_num_outputs / self.total_num_tx

            self.avg_val_inputs = self.total_val_inputs / self.total_num_inputs
            self.avg_val_outputs = self.total_val_outputs / self.total_num_outputs

    def attribute_exporter(self, only_return=False):  # Generates dict of attributes for encoding to web response.
        # Default behaviour is to also export to DB.
        export_attributes = {}
        excluded_keys = {'instantiated_block_objects', 'block_outline_list', 'failed_retrievals', 'cached_import',
                         'db_block_list', 'timestamp', 'imported_from_db'}

        for attribute, value in vars(self).items():
            if attribute not in excluded_keys:
                export_attributes[attribute] = value
        try:
            export_attributes['blocks'] = [x.hash for x in self.instantiated_block_objects]
            assert len(export_attributes['blocks']) == self.total_num_blocks
        except AssertionError:
            print(
                f"({len(export_attributes['blocks'])} "
                f"of {self.total_num_blocks}) instantiated in memory. Using DB list")
            assert self.db_block_list
            export_attributes['blocks'] = self.db_block_list

        if automatic_database_export and not only_return and not self.imported_from_db:
            try:
                blockday_collection.insert_one(export_attributes)
                return export_attributes
            except pymongo.errors.WriteError:
                print("BlockDay Write Error")
            except Exception as ex:
                print("Other BlockDay Export error occurred", ex)
        else:
            return export_attributes


class Block:
    def __init__(self, block_attr_dict, retrieved_from_db=False, transactions_required=True):
        # Can't be initialised with actual values immediately
        self.hash = ""
        self._id = ""
        self.tx = []

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

        if not retrieved_from_db:  # If block data is new then perform initialisation steps
            self.hash: str = block_attr_dict['hash']
            self._id = self.hash

            for tx in block_attr_dict['tx']:
                tx['block_hash'] = self.hash
            self.tx = [Transaction(x) for x in block_attr_dict['tx']]

            self.statistics_generation()

            if automatic_database_export:
                self.attribute_exporter()
        else:
            self.hash = block_attr_dict['_id']
            if transactions_required:
                self.retrieve_transactions_from_db()

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

    def retrieve_transactions_from_db(self):
        print(f'Retrieving Transactions for block {self.hash}')
        cursor_list = transaction_collection.find({'block_hash': self.hash})
        for tx in cursor_list:
            self.tx.append(Transaction(tx, retrieved_from_db=True))

        print(len(self.tx), self.n_tx)
        assert len(self.tx) == self.n_tx

    def statistics_generation(self):
        for transaction in self.tx:
            self.total_num_inputs_block += transaction.vin_sz
            self.total_num_outputs_block += transaction.vout_sz

            self.total_val_inputs_block += transaction.value_inputs
            self.total_val_fees_block += transaction.fee
            self.total_val_outputs_block += transaction.value_outputs

        self.average_val_inputs_per_transaction = self.total_val_inputs_block / self.total_num_inputs_block
        self.average_fee_per_transaction = self.total_val_fees_block / self.n_tx
        self.average_val_outputs_per_transaction = self.total_val_outputs_block / self.total_num_outputs_block

        self.average_num_inputs_per_transaction = self.total_num_inputs_block / self.n_tx
        self.average_num_outputs_per_transaction = self.total_num_outputs_block / self.n_tx

    def attribute_exporter(self):
        export_attributes = {}
        excluded_keys = {'tx', 'block_attribute_table', 'hash', 'cached_import', 'data_instantiated'}

        for attribute, value in vars(self).items():
            if attribute not in excluded_keys:
                export_attributes[attribute] = value

        export_attributes['tx'] = [x.hash for x in self.tx]

        if automatic_database_export:  # Block responsible for exporting transaction information as both are included
            # in the same API response, and a batch export can take place
            try:
                transaction_collection.insert_many([x.attribute_return() for x in self.tx])
                print(f"Transactions for Block {self.height} Successfully Exported")
            except pymongo.errors.WriteError:
                print("Transaction Write Error")
            except Exception as ex:
                print("Other Transaction exception occurred", ex)
            try:
                block_collection.insert_one(export_attributes)
                print(f"Block {self.height} Successfully Exported")
            except Exception as ex:
                print("Block Export Failed", ex)
        return export_attributes

    def __call__(self):
        return self


class Transaction:
    def __init__(self, transaction_attr_dict: dict, retrieved_from_db=False):

        self.coinbase_transaction: bool
        self.hash = ""  # Unique hash associated with the transaction
        self._id = ""
        self.retrieved_from_db = retrieved_from_db

        self.vin_sz: int = transaction_attr_dict['vin_sz']  # Number of inputs into the transaction
        self.vout_sz: int = transaction_attr_dict['vout_sz']  # number of outputs from the transaction
        self.size: int = transaction_attr_dict['size']
        self.time: int = transaction_attr_dict['time']
        self.block_height: int = transaction_attr_dict['block_height']
        try:
            self.block_hash = transaction_attr_dict['block_hash']
        except KeyError:
            print("Block hash inclusion skipped")
        self.fee: int = transaction_attr_dict['fee']

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
                self.attribute_return()
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
            except TypeError:
                pass
        for tr_output in self.out:
            self.value_outputs += tr_output['value']

    def attribute_return(self):
        export_attributes = {}
        excluded_keys = {'hash', 'cached_import', 'retrieved_from_db'}

        for attribute, value in vars(self).items():
            if attribute not in excluded_keys:
                export_attributes[attribute] = value
        return export_attributes

    def __call__(self, *args, **kwargs):
        return self


class Address:
    def __init__(self, address_string):

        self.address = address_string
        self._id = self.address
        self.txs = []
        self.tx_objects_instantiated = False
        self.retrieved_from_db = False

        self.n_tx: int = 0
        self.total_received: int = 0
        self.total_sent: int = 0
        self.final_balance: int = 0

        self.native_abuse_data = []

    def outline_retrieval(self):
        try:
            database_lookup = address_collection.find_one(self._id)
            assert database_lookup
        except AssertionError:
            print("Assertion Error: No Matching Address Outline found in database")
            self.bitcoin_com_api_outline_retrieval()
        else:
            self.n_tx = database_lookup['n_tx']
            self.total_received = database_lookup['total_received']
            self.total_sent = database_lookup['total_sent']
            self.final_balance = database_lookup['final_balance']
            self.txs = database_lookup['txs']
            self.db_abuse_retrieval()
            self.retrieved_from_db = True

        return self.attribute_exporter()

    def bitcoin_com_api_outline_retrieval(self):
        address_importer_url = f"https://explorer.api.bitcoin.com/btc/v1/addr/{self.address}"
        address_result = requests.get(url=address_importer_url, headers=default_headers)
        address_result.raise_for_status()
        address_data = address_result.json()

        self.n_tx = address_data['txApperances']  # Typo present in API
        self.total_received = address_data['totalReceivedSat']
        self.total_sent = address_data['totalSentSat']
        self.final_balance = address_data['balanceSat']
        self.txs = address_data['transactions']

    def tx_object_instantiation(self):
        print('Retrieving Transactions for ', self.address)
        try:
            self.db_tx_retrieval()
        except Exception as exception:
            print(f'Failed to retrieve transactions from DB {exception}')
            self.blockchain_info_api_full_tx_retrieval()
        else:
            self.tx_objects_instantiated = True

    def blockchain_info_api_tx_retrieval(self, offset):
        sleep(10)
        tx_list = []
        base_address_tx_importer_url = f"https://blockchain.info/rawaddr/{self.address}"
        instance_url = base_address_tx_importer_url + f"?offset={offset}"
        try:
            address_tx_result = requests.get(url=instance_url, headers=default_headers)
            address_tx_result.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            if not ex.response.status_code == 429:
                print('Other HTTP error occurred', ex)
        except requests.exceptions.ReadTimeout as timeout:
            print("Request read timeout", timeout)
        except requests.exceptions.ConnectionError as connection_error:
            print("Connection Error Occurred", connection_error)
        else:
            tx_list.extend(address_tx_result.json()['txs'])
        return tx_list

    def blockchain_info_api_full_tx_retrieval(self):
        tx_list = []
        for x in range(floor(self.n_tx / 50) + 1):  # Divide total count of transactions into multiples of 50
            tx_list.extend(self.blockchain_info_api_tx_retrieval(offset=x * 50))
            sleep(10)
        required_len = len(self.txs)
        self.txs = []
        for tx in tx_list:
            try:
                self.txs.append(Transaction(tx, retrieved_from_db=False))
            except KeyError:  # retrieve manually
                print(f"Key error in transaction {tx['hash']}")
                standalone_tx_url = f"https://blockchain.info/rawtx/{tx['hash']}"
                tx_info = requests.get(standalone_tx_url, headers=default_headers)
                tx_info.raise_for_status()
                self.txs.append(Transaction(tx_info.json(), retrieved_from_db=False))
                print("TX imported from API")
        print(f"{len(self.txs)} of {required_len} retrieved")
        assert len(self.txs) == required_len

    def db_tx_retrieval(self):
        assert self.txs
        raw_tx_list = deepcopy(self.txs)
        self.txs = []
        try:
            for tx in raw_tx_list:  # Retrieve TX from DB if present
                db_result = transaction_collection.find_one(tx)
                assert db_result
                self.txs.append(Transaction(db_result, retrieved_from_db=True))

            print(f"{len(self.txs)} of {len(raw_tx_list)} TX found in DB")
            assert len(self.txs) == len(raw_tx_list)
        except AssertionError:
            self.txs = raw_tx_list
            raise Exception("Unable to retrieve all TX from DB")

    def db_abuse_retrieval(self):
        try:
            native_abuse = abuse_collection.find({'address': self.address})
        except Exception as ex:
            print('Native abuse check failed', ex)
        else:
            self.native_abuse_data = [AbuseReport(each, True) for each in native_abuse]

    def attribute_exporter(self):
        export_attributes = {}
        excluded_keys = {'address', 'txs', 'tx_objects_instantiated', 'retrieved_from_db', 'native_abuse_data'}

        for attribute, value in vars(self).items():
            if attribute not in excluded_keys:
                export_attributes[attribute] = value

        export_attributes['native_abuse_data'] = [each.attribute_exporter() for each in self.native_abuse_data]

        if not self.tx_objects_instantiated:
            export_attributes['txs'] = self.txs
        else:
            export_attributes['txs'] = [tx.hash for tx in self.txs]

        if automatic_database_export and not self.retrieved_from_db:
            try:
                address_collection.save(export_attributes)
                print(f"Address Data {self.address} Successfully Exported")
            except errors.ServerSelectionTimeoutError:
                print("Address export Database Timeout")
            except pymongo.errors.WriteError:
                print("Transaction Write Error")
            except Exception as ex:
                print("Address export Failed", ex)
        # TODO: Transaction export functionality
        return export_attributes


class AbuseReport:
    def __init__(self, abuse_dict: dict, retrieved_from_db: bool = False):
        self.retrieved_from_db = retrieved_from_db

        self.address: str = abuse_dict['address']
        self.source: str = abuse_dict['source']
        self.notes: str = abuse_dict['notes']
        if self.retrieved_from_db:
            self.date = abuse_dict['date']
        else:
            self.date: datetime = datetime.strptime(abuse_dict['date'], '%Y-%m-%d')

    def attribute_exporter(self):
        export_attributes = {}
        excluded_keys = {'retrieved_from_db', '_id', 'date'}

        for attribute, value in vars(self).items():
            if attribute not in excluded_keys:
                export_attributes[attribute] = value
        if self.retrieved_from_db:
            export_attributes['date'] = self.date.strftime('%Y-%m-%d')
        else:
            export_attributes['date'] = self.date

        if automatic_database_export and not self.retrieved_from_db:
            try:
                abuse_collection.save(export_attributes)
            except errors.ServerSelectionTimeoutError:
                print("Abuse export Database Timeout")
            except Exception as ex:
                print("Abuse export Failed", ex)

        return export_attributes
