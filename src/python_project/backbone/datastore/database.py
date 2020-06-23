"""
This file contains everything related to persistence for TrustChain.
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Any

from python_project.backbone.datastore.block_store import BaseBlockStore
from python_project.backbone.datastore.chain_store import (
    BaseChain,
    BaseChainFactory,
)
from python_project.backbone.datastore.utils import (
    Dot,
    encode_raw,
    Notifier,
)


class BaseDB(ABC):
    @abstractmethod
    def get_chain(self, chain_id: bytes) -> Optional[BaseChain]:
        pass

    @abstractmethod
    def add_block(self, block: Any, block_serializer) -> None:
        pass

    @abstractmethod
    def get_block_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_tx_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass


class ChainTopic(Enum):
    ALL = 1


class DBManager(BaseDB, Notifier):
    def __init__(self, chain_factory: BaseChainFactory, block_store: BaseBlockStore):
        super().__init__()
        self.chain_factory = chain_factory
        self.block_store = block_store

        self.chains = dict()
        self.last_dots = dict()

    def get_chain(self, chain_id: bytes) -> Optional[BaseChain]:
        return self.chains.get(chain_id)

    def get_block_store(self) -> BaseBlockStore:
        return self.block_store

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

        parsed_block = block_serializer.serialize(block_blob)
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

        # Notify subs of the personal chain
        self.notify(ChainTopic.ALL, pers, pers_dots_list)

        # 2.2: add block to the community chain
        if com not in self.chains:
            self.chains[com] = self.chain_factory.create_community_chain()

        com_block_dot = Dot((parsed_block.com_seq_num, parsed_block.short_hash))
        com_dots_list = self.chains[com].add_block(parsed_block)
        full_dot_id = com + encode_raw(com_block_dot)
        self.block_store.add_dot(full_dot_id, block_hash)

        # TODO: add more chain topics
        self.notify(ChainTopic.ALL, com, com_dots_list)
