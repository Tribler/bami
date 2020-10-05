from abc import ABC, abstractmethod
from typing import Iterator, Optional, Tuple

import lmdb


class BaseBlockStore(ABC):
    """Store interface for block blobs"""

    @abstractmethod
    def add_block(self, block_hash: bytes, block_blob: bytes) -> None:
        pass

    @abstractmethod
    def iterate_blocks(self) -> Iterator[Tuple[bytes, bytes]]:
        pass

    @abstractmethod
    def get_block_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def add_tx(self, block_hash: bytes, tx_blob: bytes) -> None:
        pass

    @abstractmethod
    def add_dot(self, dot: bytes, block_hash: bytes) -> None:
        pass

    @abstractmethod
    def add_extra(self, block_hash: bytes, extra: bytes) -> None:
        pass

    @abstractmethod
    def get_extra(self, block_hash: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_hash_by_dot(self, dot: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_tx_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class LMDBLockStore(BaseBlockStore):
    """BlockStore implementation based on LMBD"""

    def __init__(self, block_dir: str) -> None:
        # Change the directory
        self.env = lmdb.open(block_dir, subdir=True, max_dbs=5, map_async=True)
        self.blocks = self.env.open_db(key=b"blocks")
        self.txs = self.env.open_db(key=b"txs")
        self.dots = self.env.open_db(key=b"dots")
        self.extra = self.env.open_db(key=b"extra")
        # add sub dbs if required

    def iterate_blocks(self) -> Iterator[Tuple[bytes, bytes]]:
        with self.env.begin() as txn:
            for k, v in txn.cursor(db=self.blocks):
                yield k, v

    def add_block(self, block_hash: bytes, block_blob: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(block_hash, block_blob, db=self.blocks)

    def add_tx(self, block_hash: bytes, tx_blob: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(block_hash, tx_blob, db=self.txs)

    def get_block_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        with self.env.begin() as txn:
            val = txn.get(block_hash, db=self.blocks)
        return val

    def get_tx_by_hash(self, block_hash: bytes) -> Optional[bytes]:
        with self.env.begin() as txn:
            val = txn.get(block_hash, db=self.txs)
        return val

    def add_dot(self, dot: bytes, block_hash: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(dot, block_hash, db=self.dots)

    def get_hash_by_dot(self, dot: bytes) -> Optional[bytes]:
        with self.env.begin() as txn:
            val = txn.get(dot, db=self.dots)
        return val

    def add_extra(self, block_hash: bytes, extra: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(block_hash, extra, db=self.extra)

    def get_extra(self, block_hash: bytes) -> Optional[bytes]:
        with self.env.begin() as txn:
            val = txn.get(block_hash, db=self.extra)
        return val

    def close(self) -> None:
        self.env.close()
