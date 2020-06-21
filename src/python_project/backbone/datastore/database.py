"""
This file contains everything related to persistence for TrustChain.
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Tuple

from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.block_store import BaseBlockStore
from python_project.backbone.datastore.chain_store import (
    Frontier,
    BaseChain,
    BaseChainFactory,
)
from python_project.backbone.datastore.utils import (
    Dot,
    encode_raw,
    Notifier,
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


class ChainTopic(Enum):
    ALL = 1


class DBManager(Notifier):
    def __init__(self, chain_factory: BaseChainFactory, block_store: BaseBlockStore):
        super().__init__()
        self.chain_factory = chain_factory
        self.block_store = block_store

        self.chains = dict()
        self.last_dots = dict()

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

    def add_block(self, block_blob: bytes, block_serializer) -> None:

        parsed_block: PlexusBlock = block_serializer.serialize(block_blob)
        block_hash = parsed_block.hash
        block_tx = parsed_block.transaction

        # 1. Add block blob and transaction blob to the block storage
        self.block_store.add_block(block_hash, block_blob)
        self.block_store.add_tx(block_hash, block_tx)

        # 2. There are two chains: personal and community chain
        pers = parsed_block.public_key
        com = parsed_block.com_id

        # 2.1: Process the block wrt personal chain
        if pers not in self.chains:
            self.chains[pers] = self.chain_factory.create_personal_chain()

        pers_block_dot = Dot((parsed_block.sequence_number, parsed_block.short_hash))
        pers_dots_list = self.chains[pers].add_block(parsed_block)
        full_dot_id = pers + encode_raw(pers_block_dot)
        self.block_store.add_dot(full_dot_id, block_hash)

        # 2.2: add block to the community chain
        if com not in self.chains:
            self.chains[com] = self.chain_factory.create_community_chain()

        com_block_dot = Dot((parsed_block.com_seq_num, parsed_block.short_hash))
        com_dots_list = self.chains[com].add_block(parsed_block)
        full_dot_id = com + encode_raw(com_block_dot)
        self.block_store.add_dot(full_dot_id, block_hash)

        # TODO: add more chain topics

        # Notify subs of the community chain
        self.notify(ChainTopic.ALL, {pers: pers_dots_list, com: com_dots_list})


class StateManager(object):
    def __init__(self, block_db: DBManager) -> None:
        self.block_db = block_db

    def process_dot_list(self, dot_dict):
        for k, v in dot_dict.keys():
            pass

    def val(self):
        self.block_db.add_observer()
