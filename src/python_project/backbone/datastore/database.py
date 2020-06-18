"""
This file contains everything related to persistence for TrustChain.
"""
from abc import ABC, abstractmethod
from typing import Optional

from python_project.backbone.block import PlexusBlock, GENESIS_HASH, GENESIS_SEQ
from python_project.backbone.datastore.block_store import BaseBlockStore
from python_project.backbone.datastore.chain_store import (
    Frontier,
    BaseChain,
    BaseChainFactory,
)
from python_project.backbone.datastore.utils import (
    Notifier,
    Dot,
    shorten,
    Links,
    encode_raw,
)


class BasePlexusDB(ABC):
    @abstractmethod
    def get_frontier(self, chain_id) -> Frontier:
        pass

    @abstractmethod
    def get_chain(self, chain_id) -> Optional[BaseChain]:
        pass

    @abstractmethod
    def add_block(self, block: PlexusBlock) -> None:
        pass

    @abstractmethod
    def get_block_by_hash(self, block_hash: bytes) -> PlexusBlock:
        pass


class DBManager(object):
    def __init__(self, chain_factory: BaseChainFactory, block_store: BaseBlockStore):
        self.chain_factory = chain_factory
        self.block_store = block_store

        self.chains = dict()
        self.last_dots = dict()

        self.notifier = Notifier()

    def _get_next_in_chain(self, chain_id: bytes, cur_dots: Links) -> Optional[Links]:
        ret_set = {}
        if chain_id not in self.chains:
            return None

        for dot in cur_dots:
            next_links = self.chains.get(chain_id).get_next_link(dot)
            if next_links:
                ret_set.update(set(next_links))
        if ret_set:
            return Links(tuple(ret_set))
        else:
            return None

    def _iter_notify(self, chain_id: bytes) -> None:
        next_links = self._get_next_in_chain(chain_id, self.last_dots.get(chain_id))
        while next_links:
            # There are next links available
            blocks = list()
            for dot in next_links:
                d = encode_raw(dot)
                b_hash = self.block_store.get_hash_by_dot(d)
                blocks.append(self.block_store.get_block_by_hash(b_hash))
            self.notifier.notify(chain_id, blocks)
            self.last_dots[chain_id] = next_links
            next_links = self._get_next_in_chain(chain_id, self.last_dots.get(chain_id))

    def add_block(self, block_blob: bytes, block_serializer) -> None:

        parsed_block: PlexusBlock = block_serializer.serialize(block_blob)
        block_hash = parsed_block.hash
        block_tx = parsed_block.transaction

        # 1. Add block to the block storage
        self.block_store.add_block(block_hash, block_blob)
        self.block_store.add_tx(block_hash, block_tx)

        # 2. Add block to the chains
        pers = parsed_block.public_key
        com = parsed_block.com_id

        # 2.1: block adding to the personal chain
        if pers not in self.chains:
            self.chains[pers] = self.chain_factory.create_chain(is_personal_chain=True)
            self.last_dots[pers] = Links(((GENESIS_SEQ, shorten(GENESIS_HASH)),))
        chain_dot = self.chains[pers].add_block(parsed_block)
        self.block_store.add_dot(pers + encode_raw(chain_dot), block_hash)

        self.chains[pers].frontier

        # iterate and notify all subscribers on the personal chain
        self._iter_notify(pers)

        # 2.2: add block to the community chain
        if com not in self.chains:
            self.chains[com] = self.chain_factory.create_chain(is_personal_chain=False)
            self.last_dots[com] = Dot((GENESIS_SEQ, shorten(GENESIS_HASH)))
        chain_dot = self.chains[com].add_block(parsed_block)
        self.block_store.add_dot(com + encode_raw(chain_dot), block_hash)

        # iterate and notify all subscribers on the community chain
        self._iter_notify(pers)
