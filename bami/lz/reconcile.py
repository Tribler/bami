from collections import defaultdict
from copy import copy
from typing import Dict, List, Optional, Set, Tuple

import mmh3
import numpy as np

from bami.lz.settings import BloomFilterSettings
from bami.lz.sketch.bloom import BloomFilter
from bami.lz.sketch.minisketch import MiniSketch, SketchError
from bami.lz.utils import bytes_to_uint


class ReconciliationManager:

    def __init__(self,
                 my_peer_id: bytes,
                 settings
                 ):
        self.all_txs = set()
        self.partners_txs = defaultdict(lambda: set())

    @property
    def known_partners(self) -> List[int]:
        return list(self.partners_txs.keys())

    def add_new_partner(self, partner_id: bytes):
        pass

    def populate_tx_set(self):
        pass


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

    def init_new_partner(self, partner_id: bytes):
        self.initialize_new_filter(partner_id)

    # On receive new transaction populate the reconciliation set
    def initialize_new_filter(self, partner_id: bytes):
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

    def settle(self, other_sketch: BloomFilter) -> Set[int]:
        candidates = []
        same_index = set(np.where((self.sketch.bits == 1) and (other_sketch.bits == 1))[0])
        for s in same_index:
            vals = self._get_txs_by_index(s)
            for tx in vals:
                if self.tx_2_cells[tx].issubset(same_index):
                    candidates.append(tx)
        return candidates

    def _get_txs_by_index(self, index: int) -> Set[int]:
        return self.cell_2_txs[index]

    def get_all_txs(self) -> List[int]:
        """Get all transactions with the indexes they occupy in the filter"""
        return list(self.tx_2_cells.keys())

    def _add_to_index(self, tx_id: int, indexes: List[int]):
        self.tx_2_cells[tx_id] = indexes
        for i in indexes:
            self.cell_2_txs[i].add(tx_id)


class MiniSketchReconciliation:

    def __init__(self,
                 my_peer_id: bytes,
                 max_size: int = 80
                 ):
        self.all_txs = set()
        self.max_size = max_size

        self.partners_txs = defaultdict(lambda: set())
        self.partner_sketches = {}

        self.common_tx = {}
        self.missing_pending = {}
        self.my_peer_id = bytes_to_uint(my_peer_id)

        self.num_sections = 1
        self.my_sketches = [MiniSketch(self.max_size)]

    def get_my_sketch(self, offset: int = 0):
        return self.my_sketches[offset]

    def change_num_sections(self, new_number: int = None):
        self.num_sections = new_number if new_number else self.num_sections * 2

        self.my_sketches = [MiniSketch(self.max_size) for _ in range(self.num_sections)]
        # repopulate sketches
        for t in self.all_txs:
            l = self.div_index(t, self.num_sections)
            self.my_sketches[l].raw_add(t)

    def div_index(self, item_val: int, total_sec: int, seed: int = 0) -> int:
        """Get cell id associated with the item"""
        if total_sec == 1:
            return 0
        return mmh3.hash(item_val.to_bytes(4, 'little'), seed) % total_sec

    def reconcile(self, other_sketch: bytes, offset: int, total: int) -> List[int]:
        """Reconcile own sketch with received other sketch.
        @return Symmetric diff
        """
        temp_sketch = MiniSketch(self.max_size)
        for l in self.all_txs:
            if self.div_index(l, total) == offset:
                temp_sketch.raw_add(l)
        temp_sketch.merge(other_sketch)
        return temp_sketch.decode()

    @property
    def known_partners(self) -> List[int]:
        return list(self.partners_txs.keys())

    def init_new_partner(self, partner_id: bytes):
        self.partners_txs[partner_id] = set()
        self.partner_sketches[partner_id] = 0

    def populate_tx(self, tx_id: int):
        self.all_txs.add(tx_id)
        i = self.div_index(tx_id, self.num_sections)
        self.my_sketches[i].raw_add(tx_id)


class BloomReconciliation:

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
