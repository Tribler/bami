from collections import defaultdict
from typing import Dict, List, Optional, Set

import numpy as np

from bami.lz.settings import BloomFilterSettings
from bami.lz.sketch.bloom import BloomFilter
from bami.lz.utils import bytes_to_uint


class CompactReconciliationSet:

    def __init__(self, sketch: BloomFilter, settled_txs: Set[int] = None):
        self.cell_2_txs = defaultdict(lambda: set())  # Filter cell -> Transaction set
        self.tx_2_cells = defaultdict(lambda: set())  # Transaction -> Cells

        self.settled_txs = set() if not settled_txs else settled_txs
        self.sketch: BloomFilter = sketch

    def add_transaction(self, tx_id: int):
        if tx_id not in self.tx_2_cells and tx_id not in self.settled_txs:
            indexes = self.sketch.add(tx_id)
            self._add_to_index(tx_id, indexes)

    def recreate_pool(self, new_filter: BloomFilter,
                      txs_to_exclude: Set[int] = None) -> 'CompactReconciliationSet':
        txs_to_exclude = set() if not txs_to_exclude else txs_to_exclude
        new_pool = CompactReconciliationSet(new_filter, txs_to_exclude)
        for tx in set(self.tx_2_cells.keys()) - txs_to_exclude:
            new_pool.add_transaction(tx)
        return new_pool

    def reconcile(self, other_sketch: BloomFilter) -> Set[int]:
        """Return transaction that peer has but not the counterparty."""
        diff = self.sketch.diff(other_sketch)
        v = np.where(diff > 0)[0]  # Something I have, but not other sketch
        ab_diff = set()
        for k in v:
            vals = self._get_txs_by_index(k)
            for tx in vals:
                ab_diff.add(tx)
        return ab_diff

    def _get_txs_by_index(self, index: int) -> Set[int]:
        return self.cell_2_txs[index]

    def get_all_txs(self) -> List[int]:
        """Get all transactions with the indexes they occupy in the filter"""
        return list(self.tx_2_cells.keys())

    def _add_to_index(self, tx_id: int, indexes: List[int]):
        self.tx_2_cells[tx_id] = indexes
        for i in indexes:
            self.cell_2_txs[i].add(tx_id)


class ReconciliationSetsManager:

    def __init__(self, my_peer_id: bytes,
                 settings: BloomFilterSettings):
        self.seeds = {}
        self.iteration = defaultdict(int)
        self.all_txs = set()
        self.recon_sets: Dict[bytes, CompactReconciliationSet] = {}
        self.my_id = bytes_to_uint(my_peer_id)
        self.bloom_size = settings.bloom_size
        self.nf = settings.bloom_num_func

        self.MAX_SEED = settings.bloom_max_seed

    @property
    def known_partners(self) -> Set[bytes]:
        return set(self.recon_sets.keys())

    # On receive new transaction populate the reconciliation set
    def initialize_new_set(self, partner_id: bytes):
        p_id = bytes_to_uint(partner_id)
        seed = self.my_id ^ p_id
        self.seeds[partner_id] = seed
        bloom_filter = BloomFilter(self.bloom_size, self.nf, seed_value=seed)
        self.recon_sets[partner_id] = CompactReconciliationSet(bloom_filter)

    def populate_reconciliation_set(self, partner_id: bytes, tx_set: Set[int]):
        for t in tx_set:
            self.recon_sets[partner_id].add_transaction(t)

    def populate_with_all_known(self, partner_id: bytes):
        for t in self.all_txs:
            self.recon_sets[partner_id].add_transaction(t)

    def populate_tx(self, tx_id: int):
        self.all_txs.add(tx_id)
        for p_id, r in self.recon_sets.items():
            r.add_transaction(tx_id)

    def iterate_reconciliation_set(self, partner_id: bytes, txs_to_remove: Set[int] = None):
        self.iteration[partner_id] = (self.iteration[partner_id] + 1) % self.MAX_SEED
        new_seed = self.seeds[partner_id] + self.iteration[partner_id]
        bloom_filter = BloomFilter(self.bloom_size, self.nf, seed_value=new_seed)
        self.recon_sets[partner_id].recreate_pool(bloom_filter, txs_to_remove)

    def get_filter(self, partner_id: bytes) -> Optional[BloomFilter]:
        return self.recon_sets[partner_id].sketch

