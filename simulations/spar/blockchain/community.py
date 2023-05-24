import math
import random
from collections import defaultdict
from hashlib import sha256
from typing import List

from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.peer import Peer

from bami.lz.utils import get_random_string, bytes_to_uint, payload_hash, uint_to_bytes
from bami.spar.base import BaseCommunity
from bami.spar.payload import TransactionPayload
from spar.blockchain.mempool import Mempool
from spar.blockchain.payload import BlockPayload, TxID, BlockRequestPayload, ReconciliationPayload


class BlockchainSPARCommunity(BaseCommunity):
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
        self.mempool = Mempool()
        self.tx_sender = {}
        self.processed_txs = set()

        self.blocks = defaultdict(set)
        self.block_payloads = {}
        self.block_sender = {}

        self.applied_blocks = []

        self.pending_txs = {}
        self.pending_blocks = []
        self.non_canon = set()

        self.can_be_applied = set()

        self.canonical_head_block = None

        self.peer_hashrate = 10
        self.block_interval = 10
        self.total_hash = 1000000

        self.max_block_txs = 100

    @property
    def my_peer_id(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def start_tx_creation(self):
        self.logger.debug("Starting dummy transaction creation")
        self.register_task(
            "create_transaction",
            self.create_transaction,
            interval=self.settings.tx_freq,
            delay=random.random() + self.settings.tx_delay,
        )

    def create_transaction(self):
        for _ in range(self.settings.tx_batch):
            script = get_random_string(self.settings.script_size)
            context = get_random_string(self.settings.script_size)
            new_tx = TransactionPayload(pk=self.my_peer_id, t_id=b'', sign=b'',
                                        script=script.encode(),
                                        context=context.encode(),
                                        fee=random.randint(0, 1000))
            t_id = bytes_to_uint(payload_hash(new_tx), self.settings.tx_id_size)
            new_tx.t_id = uint_to_bytes(t_id)

            sign = self.crypto.create_signature(self.my_peer.key,
                                                self.prepare_packet(new_tx, sig=False))
            new_tx.sign = sign
            self.process_transaction(self.my_peer, new_tx)
            # This is a new transaction - push to neighbors
            selected = random.sample(self.get_peers(),
                                     min(self.settings.initial_fanout, len(self.get_peers())))
            for p in selected:
                self.logger.debug("Sending transaction to {}".format(p))
                self.ez_send(p, new_tx, sig=False)

    @lazy_wrapper_unsigned(TransactionPayload)
    def received_transaction(self, p: Peer, payload: TransactionPayload):
        self.process_transaction(p, payload)

    def process_transaction(self, sender: Peer, tx_id: bytes, fee: int):
        # If the transaction is first received, remember the sender
        if tx_id not in self.processed_txs:
            self.tx_sender[tx_id] = sender.mid
            self.mempool.add(tx_id, fee)

    def start_mining(self):
        solve_time = -math.log(1.0 - random.random()) * (self.block_interval * self.total_hash) / self.peer_hashrate
        selected = self.mempool.select_top_n(self.max_block_txs)
        self.pending_txs = {t.t: t.f for t in selected}
        self.register_task("create_block", self.create_block, self.canonical_head_block, selected, delay=solve_time)

    def create_block(self, previous_block: BlockPayload, txs: List[TxID]):
        if not previous_block:
            prefix = b'0'
        else:
            prefix = previous_block.block_id
        new_block_hash = sha256(prefix + self.my_peer_id + b"".join([t.t for t in txs])).digest()
        seq_num = previous_block.seq_num + 1 if previous_block else 1
        self.blocks[seq_num].add(new_block_hash)
        new_block = BlockPayload(new_block_hash,
                                 txs,
                                 seq_num,
                                 self.my_peer_id,
                                 previous_block.block_id if previous_block else b'0')
        self.on_new_block(new_block)

    def recanonize(self, block: BlockPayload):
        blocks_to_apply = []
        head_block = block
        s = head_block.seq_num
        while s > len(self.applied_blocks) or self.applied_blocks[s] != head_block.block_id:
            blocks_to_apply.append(head_block)
            head_block = self.block_payloads[head_block.prev_block]

            if len(self.applied_blocks) > s:
                cur_block = self.block_payloads[self.applied_blocks[s]]
                for tx_id in cur_block.tx_list:
                    self.mempool.add(tx_id.t, tx_id.f)

        for b in reversed(blocks_to_apply):
            self.applied_blocks[b.seq_num] = b.block_id
            for tx_id in b.tx_list:
                self.mempool.remove(tx_id.t)

        self.canonical_head_block = block
        self.start_mining()

    def check_if_next_block_can_be_applied(self, block: BlockPayload):
        for b in self.blocks[block.seq_num + 1]:
            if self.block_payloads[b].prev_block == block.block_id:
                self.can_be_applied.add(b)
                val = self.check_if_next_block_can_be_applied(self.block_payloads[b])
                if val.seq_num > block.seq_num + 1:
                    return val
                else:
                    return self.block_payloads[b]
        return block

    def on_new_block(self, block: BlockPayload, from_peer: Peer = None):
        self.blocks[block.seq_num].add(block.block_id)
        self.block_payloads[block.block_id] = block
        new_header = None
        if block.prev_block == b'0' or block.prev_block in self.can_be_applied:
            self.can_be_applied.add(block.block_id)
            new_header = self.check_if_next_block_can_be_applied(block)
        else:
            if block.prev_block not in self.blocks[block.seq_num - 1]:
                self.request_blocks(from_peer, block.prev_block)

        if new_header and new_header.seq_num > self.canonical_head_block.seq_num:
            if from_peer:
                self.cancel_pending_task("create_block")
                for tx_id, fee in self.pending_txs.items():
                    self.mempool.add(tx_id, fee)
                self.pending_txs = {}
            self.recanonize(new_header)

    def send_reconciliation(self, target_peer: Peer):
        txs = [TxID(t, f) for f, _, t in self.mempool.pq]
        txs.extend([TxID(t, f) for t, f in self.pending_txs.items()])

        to_v = self.canonical_head_block.seq_num + 1
        from_v = min(self.canonical_head_block.seq_num - 10, 0)
        last_blocks_ids = [self.applied_blocks[i] for i in range(from_v, to_v)]
        reconciliation_message = ReconciliationPayload(txs, last_blocks_ids)
        self.ez_send(target_peer, reconciliation_message)

    @lazy_wrapper(ReconciliationPayload)
    def received_reconciliation(self, sender: Peer, reconciliation: ReconciliationPayload):
        for tx in reconciliation.txs:
            self.process_transaction(sender, tx.t, tx.f)
        for block_id in reconciliation.blocks:
            if block_id not in self.block_payloads:
                self.request_blocks(sender, block_id)
        self.send_reconciliation(sender)

    def request_blocks(self, peer: Peer, block_id: bytes):
        self.ez_send(peer, BlockRequestPayload(block_id))

    @lazy_wrapper(BlockRequestPayload)
    def received_block_request(self, sender: Peer, block_id: bytes):
        self.ez_send(sender, self.block_payloads[block_id])

    @lazy_wrapper(BlockPayload)
    def received_block(self, sender: Peer, block: BlockPayload):
        if block.block_id not in self.block_payloads:
            self.on_new_block(block, sender)
            self.block_sender[block.block_id] = sender.mid

