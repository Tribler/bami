from typing import Iterator, List

import mmh3
import numpy as np

from bami.lz.payload import CompactSketch


def bitarray_to_bytes(bit_array: np.ndarray) -> bytes:
    return np.packbits(bit_array).tobytes()


def bytes_to_bitarray(array_bytes: bytes) -> np.ndarray:
    return np.unpackbits(np.frombuffer(array_bytes, dtype=np.uint8))


def both_present(bf1_array: np.ndarray, bf2_array: np.ndarray) -> np.ndarray:
    return np.where((bf1_array > 0) & (bf2_array > 0))[0]


class BloomFilter:

    MAX_BITS = 32
    MAX_VAL = 2**MAX_BITS


    def __init__(self, size: int, num_func=1, seed_value: int = 0):
        self.bits = np.array([0] * size)
        self.seed_value = seed_value
        self.checksum = 0
        self.counter = 0

        self.num_func = num_func
        self.hash_seeds = []
        self.create_hash_func(num_func, seed_value)

    def __repr__(self):
        return str(self.bits)

    def create_hash_func(self, num_func: int = 1, seed_value: int = 0) -> None:
        for k in range(num_func):
            self.hash_seeds.append(seed_value + k)

    @property
    def filter_size(self):
        return len(self.bits)

    def get_indices(self, hash_val: int) -> Iterator[int]:

        bc = self.filter_size
        for h in self.hash_seeds:
            i = mmh3.hash128(str(hash_val), signed=False, seed=h) % bc
            yield i

    def add(self, hash_val: int) -> List[int]:
        """Add item to filter and return the indices"""
        indexes = []
        self.checksum = self.checksum ^ (hash_val % BloomFilter.MAX_VAL)
        self.counter += 1
        for i in self.get_indices(hash_val):
            self.bits[i] = 1
            indexes.append(i)
        return indexes

    def maybe_item(self, item_hash: int) -> bool:
        for index in self.get_indices(item_hash):
            if self.bits[index] == 0:
                return False
        return True

    def to_payload(self) -> CompactSketch:
        checksum = self.checksum.to_bytes(BloomFilter.MAX_BITS // 8, 'big', signed=False)
        return CompactSketch(self.to_bytes(), self.seed_value, checksum)

    @staticmethod
    def from_payload(payload: CompactSketch) -> 'BloomFilter':
        c = BloomFilter.from_bytes(payload.data)
        c.seed_value = payload.seed
        c.checksum = int.from_bytes(payload.csum, 'big', signed=False)
        return c

    def to_bytes(self) -> bytes:
        return bitarray_to_bytes(self.bits)

    def diff_bytes(self, other_bytes: bytes):
        return self.diff_bitarray(bytes_to_bitarray(other_bytes))

    def diff_bitarray(self, other_bitarray: np.array):
        return self.bits - other_bitarray

    def diff(self, other_bf: 'BloomFilter'):
        return self.diff_bitarray(other_bf.bits)

    @staticmethod
    def from_bytes(filter_bits: bytes) -> 'BloomFilter':
        bits = bytes_to_bitarray(filter_bits)
        size = len(bits)
        c = BloomFilter(size)
        c.bits = bits
        return c
