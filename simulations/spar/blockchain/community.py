from collections import defaultdict
from hashlib import sha256
import math
import random
from typing import List

from ipv8.peer import Peer

from bami.spar.community import SPARCommunity
from bami.spar.payload import TransactionPayload
from spar.blockchain.mempool import Mempool


class BlockchainSPARCommunity(SPARCommunity):
    """Community to exchange transactions and blocks on the network overlay for a
    Bitcoin-like blockchain.
    1. Received transactions from some client. For the simulation we generate transaction at each peer randomly.
    2. Create a block. Share the block with the neighbors.
    3. Exchange blocks between each other. Syncronizing the blockchain.
    When peer receives a message (transaction or block) it will remember the sender of the message.
    Later the message is evaluated against the usefullness of the message. If the message is usefull, the sender is
    rewarded.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # heap mempool
        self.mempool = Mempool()
        self.tx_payloads = {}
        self.tx_sender = {}

        self.blocks = defaultdict(list)

        self.peer_hashrate = 10
        self.block_interval = 10
        self.total_hash = 1000000

        self.max_block_txs = 100

    def received_transaction(self, sender: Peer, tx: TransactionPayload):
        # If the transaction is first received, remember the sender
        if tx.t_id not in self.tx_payloads:
            self.tx_payloads[tx.t_id] = tx
            self.tx_sender[tx.t_id] = sender
            self.mempool.add(tx.t_id, tx.fee)

    def start_mining(self):
        solve_time = -math.log(1.0 - random.random()) * (self.block_interval * self.total_hash) / self.peer_hashrate
        selected_txs = self.mempool.select_top_n(self.max_block_txs)
        seq_num = max(self.blocks.keys(), default=0) + 1
        self.register_task("create_block", self.create_block, seq_num, selected_txs, delay=solve_time)

    def create_block(self, seq_num: int, txs: List[bytes]):
        block_hash = sha256(self.my_peer_id + b"".join(txs)).digest()



