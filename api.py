from concurrent.futures import as_completed

import datetime
import requests
from requests_futures.sessions import FuturesSession
from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from time import sleep

default_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.74 Safari/537.36 Edg/79.0.309.43'}

console = Console()


def json_getter(url, headers, session):
    try:
        r = session.get(url=url, headers=headers, timeout=30)
        raw_json_request = r.json()
        console.print("Retrieved Data from {}".format(url))
        return raw_json_request
    except:
        console.print("Error when requesting data.")


def date_importer():
    date_object = None
    valid_input = False
    while not valid_input:
        year_selection = console.input("Please Input Year: ")
        month_selection = console.input("Please Input Month: ")
        day_selection = console.input("Please Input Day: ")

        if year_selection.isdigit() and month_selection.isdigit() and day_selection.isdigit():

            date_string = year_selection + month_selection + day_selection
            date_format = '%Y%m%d'
            try:
                date_object: datetime = datetime.datetime.strptime(date_string, date_format)
                valid_input = True
            except ValueError:
                console.print("[bold red]Ensure date is input in format 2020 04 15")
                continue
        else:
            continue

    timestamp_in_milliseconds = date_object.timestamp()*1000
    timestamp_as_string = str(timestamp_in_milliseconds)[:-2]
    block_importer_url = f'https://blockchain.info/blocks/{timestamp_as_string}?format=json'
    with requests.session() as session:
        block_data_dict = json_getter(url=block_importer_url, headers=default_headers, session=session)
    block_data_list = block_data_dict['blocks']

    block_day = BlockDay(block_data_list, timestamp_in_milliseconds)
    block_day.instantiate_blocks()


def console_menu():
    # noinspection PyTypeChecker
    options_table = Table(title=Panel('Main Menu'))

    options_table.add_column("Key")
    options_table.add_column("Function")

    options_table.add_row("1", "Calculate Transaction Info For Day")

    console.print(Align.center(options_table))

    print(date_importer())

    # main_menu_selection = console.input('Please Select an Option: ')
    #
    # main_menu_dict = {
    #     '1': [console.print(Panel('Block Importer by Date')), date_importer()]
    # }


class BlockDay:
    def __init__(self, block_data_list, timestamp):
        self.block_data_list = block_data_list
        self.block_object_dict: dict = {}
        self.timestamp: datetime = timestamp

    def instantiate_blocks(self):

        with FuturesSession() as session:
            futures = []
            for x in self.block_data_list:
                block_hash = x['hash']
                print(f'Current block hash is {block_hash}')
                block_api_single_block_url = f'https://blockchain.info/rawblock/{block_hash}'

                futures.append(session.get(url=block_api_single_block_url, headers=default_headers))
                # sleep(0.5)
            for future in as_completed(futures):
                result = future.result()
                block_dict = result.json()

                block_object = Block(block_dict)
                block_object.attribute_print()
                self.block_object_dict.update({block_object.hash: block_object})


class Block:
    def __init__(self, block_dict):
        self.ver: int = block_dict['ver']
        self.time: int = block_dict['time']  # Timestamp of the block (unix format)
        self.bits: int = block_dict['bits']
        self.fee: int = block_dict['fee']
        self.nonce: int = block_dict['nonce']
        self.n_tx: int = block_dict['n_tx']
        self.size: int = block_dict['size']
        self.main_chain: bool = block_dict['main_chain']
        self.height: int = block_dict['height']
        self.weight: int = block_dict['weight']
        self.hash: str = block_dict['hash']
        self.prev_block: str = block_dict['prev_block']
        self.mrkl_root: str = block_dict['mrkl_root']
        self.transaction_list: list = [Transaction(x, self.hash) for x in block_dict['tx']]

        self.block_attribute_table = Table(title=Panel('Block Attributes'))

    def attribute_print(self):
        self.block_attribute_table.add_column("Attribute")
        self.block_attribute_table.add_column("Value")

        excluded_keys={"transaction_list","block_attribute_table"}

        for attribute in vars(self):
            if attribute not in excluded_keys:
                self.block_attribute_table.add_row(str(attribute), str(vars(self)[attribute]))
        console.print(self.block_attribute_table)



class Transaction:
    def __init__(self, transaction_dict, block_hash):
        self.hash: str = transaction_dict['hash']  # Unique hash associated with the transaction
        self.ver: int = transaction_dict['ver']  # Version of the transaction
        self.vin_sz: int = transaction_dict['vin_sz']  # Number of inputs into the transaction
        self.vout_sz: int = transaction_dict['vout_sz']  # number of outputs from the transaction
        self.size: int = transaction_dict['size']
        self.fee: int = transaction_dict['fee']
        self.double_spend: bool = transaction_dict['double_spend']
        self.time: int = transaction_dict['time']
        self.block_height: int = transaction_dict['block_height']
        self.block_hash: str = block_hash
        self.inputs: list = transaction_dict['inputs']
        self.out: list = transaction_dict['out']

        self.transaction_attribute_table = Table(title=Panel('Transaction Attributes'))
        self.attribute_print()


    def attribute_print(self):
        self.transaction_attribute_table.add_column("Attribute")
        self.transaction_attribute_table.add_column("Value")

        excluded_keys = {"inputs", "out"}

        for attribute in vars(self):
            if attribute not in excluded_keys:
                self.transaction_attribute_table.add_row(str(attribute), str(vars(self)[attribute]))
        console.print(self.transaction_attribute_table)


if __name__ == "__main__":
    # console_menu()
    pass
