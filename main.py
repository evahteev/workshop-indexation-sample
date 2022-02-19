from eth_abi.exceptions import InsufficientDataBytes

from evm_client import EVMClient


WEB3_ENDPOINT = 'YOUR_NODE_ADDRESS_HOES_THERE'

evm_client = EVMClient(WEB3_ENDPOINT)


def main():
    latest_block = evm_client.get_latest_block_number()
    tx_indices_for_latest_block = evm_client.get_transactions_indexes_for_block(latest_block)
    transactions_for_block = []
    for tx_index in tx_indices_for_latest_block:
        transaction = evm_client.get_transaction_for_block_by_index(latest_block, tx_index)
        transactions_for_block.append(transaction)

    transactions_receipts = {}
    for transaction in transactions_for_block:
        transactions_receipts[transaction['address']] = evm_client.get_transaction_receipt(transaction['address'])

    topic_keccaks, topic_indexed_types, topic_indexed_names, topic_names, topic_types = \
        evm_client.get_contract_topics_types_names(evm_client.erc20_contract_abi_dict)

    parsed_transfers = []
    for receipt in transactions_receipts.values():
        if receipt and receipt.logs:
            for receipt_log in receipt.logs:
                try:
                    topic = receipt_log.topics[0][0:4]
                except IndexError:
                    print(f'Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}')
                    continue

                if topic == b'\xdd\xf2R\xad':
                    # Transfer here detected
                    try:
                        token_address = receipt_log['address']
                        parsed_transfer = evm_client.parse_event(topic_indexed_types, topic_types, topic_names,
                                                                 topic_indexed_names,
                                                                 'Transfer',
                                                                 receipt_log)
                        parsed_transfers.append({**{'token_address': token_address}, **parsed_transfer})
                    except InsufficientDataBytes:
                        print('Cant parse transfer from receipt log erc721 transfer')
                        continue

    print(f'Got {len(parsed_transfers)} example {parsed_transfers[0]} ready to be saved in any DB')

if __name__ == '__main__':
    main()
