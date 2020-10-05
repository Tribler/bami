from typing import Iterator, Optional, Iterable, Set, Tuple

from bami.backbone.block import BamiBlock
from bami.backbone.datastore.block_store import BaseBlockStore
from bami.backbone.datastore.chain_store import (
    BaseChain,
    FrontierDiff,
    Frontier,
    BaseChainFactory,
)
from bami.backbone.datastore.database import BaseDB
from bami.backbone.utils import Dot, Links, ShortKey


class MockBlockStore(BaseBlockStore):
    def iterate_blocks(self) -> Iterator[Tuple[bytes, bytes]]:
        return []

    def add_extra(self, block_hash: bytes, extra: bytes) -> None:
        pass

    def get_extra(self, block_hash: bytes) -> Optional[bytes]:
        pass

    def close(self) -> None:
        pass

    def add_block(self, block_hash: bytes, block_blob: bytes) -> None:
        pass

    def get_block_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        pass

    def add_tx(self, block_hash: bytes, tx_blob: bytes) -> None:
        pass

    def add_dot(self, dot: bytes, block_hash: bytes) -> None:
        pass

    def get_hash_by_dot(self, dot: bytes) -> Optional[bytes]:
        pass

    def get_tx_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        pass


class MockDBManager(BaseDB):
    def get_last_reconcile_point(self, chain_id: bytes, peer_id: bytes) -> int:
        pass

    def set_last_reconcile_point(
        self, chain_id: bytes, peer_id: bytes, last_point: int
    ) -> None:
        pass

    def get_extra_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    def has_block(self, block_hash: bytes) -> bool:
        return False

    def store_last_frontier(
        self, chain_id: bytes, peer_id: bytes, frontier: Frontier
    ) -> None:
        pass

    def get_last_frontier(self, chain_id: bytes, peer_id: bytes) -> Frontier:
        pass

    def get_block_blobs_by_frontier_diff(
        self, chain_id: bytes, frontier_diff: FrontierDiff, vals_to_request: Set
    ) -> Iterable[bytes]:
        pass

    def close(self) -> None:
        pass

    @property
    def chain_factory(self) -> BaseChainFactory:
        return MockChainFactory()

    @property
    def block_store(self) -> BaseBlockStore:
        return MockBlockStore()

    def get_chain(self, chain_id) -> Optional[BaseChain]:
        pass

    def add_block(self, block: BamiBlock, block_serializer) -> None:
        pass

    def get_block_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass

    def get_tx_blob_by_dot(self, chain_id: bytes, block_dot: Dot) -> Optional[bytes]:
        pass


class MockChain(BaseChain):
    @property
    def terminal(self) -> Links:
        pass

    def get_all_short_hash_by_seq_num(self, seq_num: int) -> Iterable[ShortKey]:
        pass

    def get_dots_by_seq_num(self, seq_num: int) -> Iterable[Dot]:
        pass

    def add_block(
        self, block_links: Links, block_seq_num: int, block_hash: bytes
    ) -> Dot:
        pass

    def reconcile(self, frontier: Frontier, lrp: int = None) -> FrontierDiff:
        pass

    @property
    def frontier(self) -> Frontier:
        pass

    @property
    def consistent_terminal(self) -> Links:
        pass

    def get_next_links(self, block_dot: Dot) -> Optional[Links]:
        pass

    def get_prev_links(self, block_dot: Dot) -> Optional[Links]:
        pass


class MockChainFactory(BaseChainFactory):
    def create_chain(self, **kwargs) -> BaseChain:
        return MockChain()
