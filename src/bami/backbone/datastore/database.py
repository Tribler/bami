"""
This file contains everything related to persistence for TrustChain.
"""
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Dict, Iterable, Optional, Set, Tuple

from bami.backbone.datastore.block_store import BaseBlockStore
from bami.backbone.datastore.chain_store import (
    BaseChain,
    BaseChainFactory,
    Frontier,
    FrontierDiff,
)
from bami.backbone.utils import (
    Dot,
    EMPTY_PK,
    encode_raw,
    expand_ranges,
    Links,
    Notifier,
    ShortKey,
)


class BaseDB(ABC, Notifier):
    @abstractmethod
    def get_chain(self, chain_id: bytes) -> Optional[BaseChain]:
        pass

    @abstractmethod
    def add_block(
        self, block_blob: bytes, block: "PlexusBlock", prefix: bytes = None
    ) -> None:
        pass

    @abstractmethod
    def get_block_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_tx_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_extra_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    @property
    @abstractmethod
    def chain_factory(self) -> BaseChainFactory:
        pass

    @property
    @abstractmethod
    def block_store(self) -> BaseBlockStore:
        pass

    @abstractmethod
    def has_block(self, block_hash: bytes) -> bool:
        pass

    @abstractmethod
    def get_last_reconcile_point(self, chain_id: bytes, peer_id: bytes) -> int:
        pass

    @abstractmethod
    def set_last_reconcile_point(
        self, chain_id: bytes, peer_id: bytes, last_point: int
    ) -> None:
        pass

    def reconcile(
        self, chain_id: bytes, frontier: Frontier, peer_id: bytes
    ) -> FrontierDiff:
        """Reconcile the frontier from peer. If chain does not exist - will create chain and reconcile."""
        chain = self.get_chain(chain_id)
        if not chain:
            chain = self.chain_factory.create_chain()
        res = chain.reconcile(
            frontier, self.get_last_reconcile_point(chain_id, peer_id)
        )
        if res.is_empty():
            # The frontiers are same => update reconciliation point
            self.set_last_reconcile_point(chain_id, peer_id, max(frontier.terminal)[0])
        return res

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def get_block_blobs_by_frontier_diff(
        self, chain_id: bytes, frontier_diff: FrontierDiff, vals_to_request: Set
    ) -> Iterable[bytes]:
        pass


class ChainTopic(Enum):
    ALL = 1
    PERSONAL = 2
    GROUP = 3


class DBManager(BaseDB):
    def __init__(self, chain_factory: BaseChainFactory, block_store: BaseBlockStore):
        super().__init__()
        self._chain_factory = chain_factory
        self._block_store = block_store

        self.chains = dict()
        self.last_reconcile_seq_num = defaultdict(lambda: defaultdict(int))

    def get_last_reconcile_point(self, chain_id: bytes, peer_id: bytes) -> Links:
        return self.last_reconcile_seq_num[chain_id][peer_id]

    def set_last_reconcile_point(
        self, chain_id: bytes, peer_id: bytes, last_point: int
    ) -> None:
        self.last_reconcile_seq_num[chain_id][peer_id] = last_point

    def _process_missing_seq_num(
        self, chain: BaseChain, chain_id: bytes, missing_ranges: Set[int]
    ) -> Iterable[bytes]:
        for b_i in missing_ranges:
            # Return all blocks with a sequence number
            for dot in chain.get_dots_by_seq_num(b_i):
                val = self.get_block_blob_by_dot(chain_id, dot)
                if not val:
                    raise Exception("No block", chain_id, dot)
                yield val

    def _find_first_conflicting_point(
        self, conf_dict: Dict, chain: BaseChain
    ) -> Set[Dot]:
        to_request = set()
        for sn, hash_vals in conf_dict.items():
            local_val = chain.get_all_short_hash_by_seq_num(sn)
            if not local_val:
                # Don't know this value => request from peer
                to_request.update(Dot((sn, k) for k in hash_vals))
                continue
            diff_val = local_val - set(hash_vals)
            sim_diff = set(hash_vals) - local_val
            if sim_diff:
                to_request.update(Dot((sn, k)) for k in sim_diff)
            # If there is a hash that is known
            if diff_val:
                # First inconsistency point met
                return {Dot((sn, k)) for k in diff_val}, to_request
        return set(), to_request

    def _process_conflicting(
        self,
        chain: BaseChain,
        chain_id: bytes,
        conflict_dict: Dict[Dot, Dict[int, Tuple[ShortKey]]],
        val_to_request: Set,
    ):
        # for c in conflict_dict:
        #    val = self.get_block_blob_by_dot(chain_id, c)
        #    if not val:
        #        raise Exception("No block", chain_id, c)
        #    yield val
        for conf_dot, conf_dict in conflict_dict.items():
            if not conf_dict:
                val = self.get_block_blob_by_dot(chain_id, conf_dot)
                if not val:
                    raise Exception("No block", chain_id, conf_dot)
                yield val
                continue
            current_point, to_request = self._find_first_conflicting_point(
                conf_dict, chain
            )
            val_to_request.update(to_request)
            while (
                current_point
                and max(current_point)[0] < conf_dot[0]
                and conf_dot not in current_point
            ):
                new_point = set()
                for d in current_point:
                    val = self.get_block_blob_by_dot(chain_id, d)
                    if not val:
                        raise ("Exeption no val", val, chain_id, d)
                    yield val
                    l = chain.get_next_links(d)
                    if l:
                        new_point.update(set(l))
                current_point = new_point
            val = self.get_block_blob_by_dot(chain_id, conf_dot)
            if not val:
                raise Exception("Exception not val", chain_id, conf_dot)
            yield val

    def get_block_blobs_by_frontier_diff(
        self, chain_id: bytes, frontier_diff: FrontierDiff, vals_to_request: Set
    ) -> Iterable[bytes]:
        chain = self.get_chain(chain_id)
        print("Getting blocks by diff ", frontier_diff, " on chain ", chain_id)
        if chain:
            # Processing missing holes
            blks = set(
                self._process_missing_seq_num(
                    chain, chain_id, expand_ranges(frontier_diff.missing)
                )
            )
            print("Missing blocks ", len(blks))
            blks.update(
                set(
                    self._process_conflicting(
                        chain, chain_id, frontier_diff.conflicts, vals_to_request
                    )
                )
            )
            print("Total blocks ", len(blks))
            return blks
        return []

    def close(self) -> None:
        self.block_store.close()

    @property
    def chain_factory(self) -> BaseChainFactory:
        return self._chain_factory

    @property
    def block_store(self) -> BaseBlockStore:
        return self._block_store

    def get_chain(self, chain_id: bytes) -> Optional[BaseChain]:
        return self.chains.get(chain_id)

    def get_block_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        dot_id = chain_id + encode_raw(block_dot)
        blk_hash = self.block_store.get_hash_by_dot(dot_id)
        if blk_hash:
            return self.block_store.get_block_by_hash(blk_hash)
        else:
            return None

    def get_tx_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        dot_id = chain_id + encode_raw(block_dot)
        hash_val = self.block_store.get_hash_by_dot(dot_id)
        if hash_val:
            return self.block_store.get_tx_by_hash(hash_val)
        else:
            return None

    def get_extra_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        dot_id = chain_id + encode_raw(block_dot)
        hash_val = self.block_store.get_hash_by_dot(dot_id)
        if hash_val:
            return self.block_store.get_extra(hash_val)
        else:
            return None

    def has_block(self, block_hash: bytes) -> bool:
        return self.block_store.get_block_by_hash(block_hash) is not None

    def add_block(
        self, block_blob: bytes, block: "PlexusBlock", prefix: bytes = None
    ) -> None:

        block_hash = block.hash
        block_tx = block.transaction

        # 1. Add block blob and transaction blob to the block storage
        self.block_store.add_block(block_hash, block_blob)
        self.block_store.add_tx(block_hash, block_tx)
        self.block_store.add_extra(block_hash, encode_raw({b"type": block.type}))

        # 2. There are two chains: personal and community chain
        pers = block.public_key
        com = block.com_id

        if prefix:
            if com == pers:
                pers = prefix + pers
            com = prefix + com

        print("Adding block ", block.com_dot, " to chain ", pers, " and ", com)

        # 2.1: Process the block wrt personal chain
        if pers not in self.chains:
            self.chains[pers] = self.chain_factory.create_chain()

        pers_block_dot = Dot((block.sequence_number, block.short_hash))
        pers_dots_list = self.chains[pers].add_block(
            block.previous, block.sequence_number, block_hash
        )
        full_dot_id = pers + encode_raw(pers_block_dot)
        self.block_store.add_dot(full_dot_id, block_hash)
        # TODO: add more chain topic

        # Notify subs of the personal chain
        self.notify(ChainTopic.ALL, chain_id=pers, dots=pers_dots_list)
        self.notify(ChainTopic.PERSONAL, chain_id=pers, dots=pers_dots_list)
        self.notify(pers, chain_id=pers, dots=pers_dots_list)

        # 2.2: add block to the community chain

        if com != EMPTY_PK:
            if com == pers:
                # Chain was processed already, notify rest
                self.notify(ChainTopic.GROUP, chain_id=com, dots=pers_dots_list)
            else:
                if com not in self.chains:
                    print("Creating community chain", com)
                    self.chains[com] = self.chain_factory.create_chain()
                com_block_dot = Dot((block.com_seq_num, block.short_hash))
                com_dots_list = self.chains[com].add_block(
                    block.links, block.com_seq_num, block_hash
                )
                full_dot_id = com + encode_raw(com_block_dot)
                self.block_store.add_dot(full_dot_id, block_hash)

                self.notify(ChainTopic.ALL, chain_id=com, dots=com_dots_list)
                self.notify(ChainTopic.GROUP, chain_id=com, dots=com_dots_list)
                self.notify(com, chain_id=com, dots=com_dots_list)
