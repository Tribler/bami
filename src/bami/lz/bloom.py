from typing import List, Iterator

import mmh3
import numpy as np


def extractKBytes(num: int, k: int, max_len: int = 32) -> int:
    v = int.to_bytes(num, max_len, 'big')
    return int.from_bytes(v[max_len - k:max_len], 'big')


def bitarray_to_bytes(bit_array: np.ndarray) -> bytes:
    return np.packbits(bit_array).tobytes()


def bytes_to_bitarray(array_bytes: bytes) -> np.ndarray:
    return np.unpackbits(np.frombuffer(array_bytes, dtype=np.uint8))


def both_present(bf1_array: np.ndarray, bf2_array: np.ndarray) -> np.ndarray:
    return np.where((bf1_array > 0) & (bf2_array > 0))[0]


class BloomFilter:

    def __init__(self, size: int, num_func=1, seed_value: int = 0):
        self.bits = np.array([0] * size)
        self.seed_value = seed_value

        self.num_func = num_func
        self.hash_func = []
        self.create_hash_func(num_func, seed_value)

    def create_hash_func(self, num_func: int = 1, seed_value: int = 0) -> None:
        for k in range(num_func):
            self.hash_func.append(mmh3.hash128(bytes(k), seed=seed_value))

    @property
    def filter_size(self):
        return len(self.bits)

    def get_indices(self, hash_val: int) -> Iterator[int]:
        bc = self.filter_size
        for h in self.hash_func:
            i = (h ^ hash_val) % bc
            yield i

    def add(self, hash_val: int) -> List[int]:
        """Add item to filter and return the indices"""
        indexes = []
        for i in self.get_indices(hash_val):
            self.bits[i] = 1
            indexes.append(i)
        return indexes

    def maybe_item(self, item_hash: int) -> bool:
        for index in self.get_indices(item_hash):
            if self.bits[index] == 0:
                return False
        return True

    def to_bytes(self) -> bytes:
        return bitarray_to_bytes(self.bits)

# CountingBloom Filter to have as an option.
