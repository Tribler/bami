"""
This file contains everything related to persistence for TrustChain.
"""
from abc import ABC, abstractmethod
from typing import Optional

from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.chain_store import Frontier, BaseChain


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
