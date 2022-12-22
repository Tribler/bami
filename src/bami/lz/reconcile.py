import logging
import random
from binascii import hexlify
from collections import defaultdict
from typing import Set, List, Dict

from bami.lz.base import BaseMixin
from bami.lz.bloom import BloomFilter
from bami.lz.payload import TxsChallengePayload


class TransactionFiltersIndexes:

    """This class stores which positions in the filter each transaction occupies.
    It also stores an inverted index to get access to all transactions in a certain filter.
    """

    def __init__(self):
        self.filter_cell_2_txs = defaultdict(lambda: defaultdict(lambda: set()))  # Filter Cell -> Transaction Set
        self.tx_2_filter_cells = defaultdict(lambda: dict())  # Transaction id -> Which Filter Cells
        self.unprocessed_txs = defaultdict(lambda: set())  # non-reconciled transactions

    def add_new_tx(self, filter_id: bytes, tx_hash: int):
        self.unprocessed_txs[filter_id].add(tx_hash)

    def get_txs_by_index(self, filter_id: bytes, index: int) -> Set[int]:
        return self.filter_cell_2_txs[filter_id][index]

    def get_all_txs_by_filter(self, filter_id: bytes) -> Dict[int, List[int]]:
        """Get all transactions with the indexes they occupy in the filter"""
        return self.tx_2_filter_cells[filter_id]

    def add_to_index(self, tx_hash: int, filter_id: bytes, indexes: List[int]):
        self.tx_2_filter_cells[filter_id][tx_hash] = indexes
        for i in indexes:
            self.filter_cell_2_txs[filter_id][i].add(tx_hash)


class FiltersPool:

    def __init__(self, my_peer_id: bytes, size=8 * 100, num_func=3):
        self.filters = {}

        self.my_id = int(hexlify(my_peer_id), 16)
        self.MOD_VAL = 255

        self.size = size
        self.num_func = num_func

        self.tx_filters_indexes = TransactionFiltersIndexes()

    def get_initial_seed(self, other_peer_id: bytes) -> int:
        c = hexlify(other_peer_id)
        seed = int(c, 16) + self.my_id + 0
        seed = seed % self.MOD_VAL
        return seed

    def get_filter(self, other_peer_id: bytes) -> BloomFilter:
        filter_id = other_peer_id
        blm_filter = self.filters.get(filter_id)
        if not blm_filter:
            blm_filter = BloomFilter(self.size,
                                     self.num_func,
                                     seed_value=self.get_initial_seed(other_peer_id))
            self.filters[filter_id] = blm_filter
        return blm_filter

    # Get filter with new seed

    def increment_seed(self, old_seed: int) -> int:
        return (old_seed + 1) % self.MOD_VAL

    def update_filter(self, other_peer_id: bytes, tx_hash: int):
        f_id = other_peer_id
        indexes = self.get_filter(f_id).add(tx_hash)
        # Recheck
        self.tx_filters_indexes.add_to_index(tx_hash, f_id, indexes)






class Reconciliation(BaseMixin):

    def start_reconciliation(self):
        self.register_task(
            "reconciliation",
            self.reconcile_with_neighbors,
            interval=self.settings.recon_freq,
            delay=random.random() + self.settings.recon_delay,
        )

    def reconcile_with_neighbors(self):
        my_state = self.peer_db.get_peer_txs(self.my_peer_id)
        f = self.settings.recon_fanout
        selected = random.sample(self.get_peers(), min(f, len(self.get_peers())))
        for p in selected:
            p_id = p.public_key.key_to_bin()
            peer_state = self.peer_db.get_peer_txs(p_id)
            set_diff = my_state - peer_state
            if len(set_diff) > 0:
                request = TxsChallengePayload([TxId(s) for s in set_diff])
                self.ez_send(p, request, sign=False)

    # Reconciliation procedure:
    # 1. Transactions are put to
    # What is there is block restructuring? - There is a change in the transaction inclusion?
    # Can be two transaction sets - all seen vs valid.
    # The transaction sets are - clock and pairwise filter.
    # With the filter get a checksum for each corresponding cell.

    # CellState
    # BloomFilter
