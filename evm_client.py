import json
import logging
import eth_abi

from typing import List, Optional, Union

from hexbytes import HexBytes
from web3 import Web3, exceptions as web3_exceptions, HTTPProvider
from web3.exceptions import BlockNotFound
from eth_utils import decode_hex
from web3.middleware import geth_poa_middleware

logger = logging.getLogger(__name__)


class EVMClient:

    def __init__(self, web3_url: str):
        self.w3 = Web3(HTTPProvider(web3_url))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        erc20_abi_path = 'abi/ERC20.json'
        with open(erc20_abi_path) as fh:
            abi_contract_dict = json.load(fh)
        self.erc20_contract_abi_dict = abi_contract_dict
        self.erc20_contract = self.w3.eth.contract(abi=abi_contract_dict)

    def get_latest_block_number(self) -> int:
        """
        Gets latest block number
        :return: int latest block number
        """

        return self.w3.eth.blockNumber

    def get_block_timestamp(self, block_identifier: Union[str, int]) -> int:
        """
        Gets block's timestamp

        :param block_identifier: block number(int) or 'latest'
        :return: int timestamp of the block
        """
        return self.w3.eth.getBlock(block_identifier)['timestamp']

    def get_transactions_indexes_for_block(self, block_identifier: Union[str, int]) -> List[int]:
        """
        Gets transactions indexes block

        :param block_identifier: block number(int) or 'latest'
        :return: list of transactions indexes for block
        """
        try:
            transactions_count = self.w3.eth.getBlockTransactionCount(block_identifier)
        except BlockNotFound:
            logger.error("Cant find block on the node")
            raise BlockNotFound
        transactions_idxs = []
        for transaction_index in range(0, transactions_count, 1):
            transactions_idxs.append(transaction_index)
        return transactions_idxs

    def get_transaction_for_block_by_index(self, block_identifier: Union[str, int],
                                           transaction_index: int,
                                           block_timestamp: Optional[int] = None) -> Optional[dict]:
        """
        Gets transactions for block

        :param block_identifier: block number(int) or 'latest'
        :transaction_index: index of transaction in block
        :param block_timestamp: block timestamp(int)
        :return: list of transactions for block
        """
        transaction = self.w3.eth.getTransactionByBlock(block_identifier, transaction_index)
        if not block_timestamp:
            block_timestamp = self.get_block_timestamp(block_identifier)
        transaction = {**transaction, **{'timestamp': block_timestamp,
                                         'address': transaction['hash'].hex(),
                                         'value': transaction['value']}}
        return transaction

    def get_transaction_receipt(self, transaction_hash: HexBytes) -> Optional[dict]:
        try:
            receipt = self.w3.eth.waitForTransactionReceipt(transaction_hash, timeout=20)
        except web3_exceptions.TimeExhausted as e:
            logger.error(f"Timed out waiting for transactions receipt {e}")
            return None
        if receipt.status != 1:
            logger.debug(f"Cant get receipt, wrong status {receipt.status}")
            return None
        return receipt

    @staticmethod
    def get_contract_topics_types_names(abi_contract: dict):
        topic_keccaks = {}
        topic_types = {}
        topic_names = {}
        topic_indexed_types = {}
        topic_indexed_names = {}
        for element in abi_contract:
            if element.get('name'):
                all_types = []
                indexed_types = []
                indexed_names = []
                types = []
                names = []

                for input in element['inputs']:
                    all_types.append(input["type"])
                    if input.get('indexed'):
                        indexed_types.append(input["type"])
                        indexed_names.append(input["name"])
                    else:
                        types.append(input["type"])
                        names.append(input["name"])

                joined_input_types = ",".join(input for input in all_types)
                topic_keccaks[Web3.keccak(text=f"{element['name']}({joined_input_types})")[0:4]] \
                    = element['name']
                topic_types[element['name']] = types
                topic_names[element['name']] = names
                topic_indexed_types[element['name']] = indexed_types
                topic_indexed_names[element['name']] = indexed_names

        return topic_keccaks, topic_indexed_types, topic_indexed_names, topic_names, topic_types


    @staticmethod
    def parse_event(topic_indexed_types, topic_types, topic_names, topic_indexed_names,
                    event_name: str, receipt_log):
        encoded_topics = [decode_hex(Web3.toHex(topic)) for topic in receipt_log.topics[1:]]
        indexed_values = [eth_abi.decode_single(t, v) for t, v in
                          zip(topic_indexed_types[event_name], encoded_topics)]
        values = eth_abi.decode_abi(topic_types[event_name], decode_hex(receipt_log.data))

        return {**{**dict(zip(topic_names[event_name], values)),
                   **dict(zip(topic_indexed_names[event_name], indexed_values))}}
