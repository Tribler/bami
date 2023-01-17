from copy import copy
from typing import Any, Iterable, List

from bami.lz.sketch.minisketch import MiniSketch, SketchError

import mmh3


class CMS:

    def __init__(self, n: int, m: int, seed: int = 0):
        self.n = n
        self.seed = seed
        self.minis = [MiniSketch(m) for _ in range(n)]
        self.sketch_salt = self.seed.to_bytes(4, 'little')

    def get_index(self, item):
        return mmh3.hash(item.to_bytes(8, 'little'), self.seed) % self.n

    def add(self, item: int):
        v = self.get_index(item)
        self.minis[v].raw_add(item)

    def serialize(self):
        return [self.minis[v].serialize() for v in range(self.n)]

    def reconcile(self, minis_list: List[bytes]) -> List[int]:
        res = []
        for k in range(self.n):
            v = copy(self.minis[k])
            v.merge(minis_list[k])
            try:
                vals = v.decode()
                for j in vals:
                    if self.get_index(j) != k:
                        res.append(None)
                        continue
                res.append(vals)
            except SketchError:
                res.append(None)
        return res


class PBS:

    def __init__(self, n: int, m: int, seed: int = 0):
        self.n = n
        self.mini = MiniSketch(m)
        self.seed = seed
        self.xor_sums = [0 for _ in range(n)]
        self.checksum = 0

    def index(self, item: int):
        return mmh3.hash(item.to_bytes(8, 'little'), self.seed) % self.n

    def add(self, item: int):
        v = self.index(item)
        self.mini.raw_add(v + 1)
        self.xor_sums[v] ^= item
        self.checksum += item

    def identify(self, other_mini: bytes) -> List[int]:
        val = copy(self.mini)
        val.merge(other_mini)
        return val.decode()

    def reconcile(self, indexes: List[int], xor_sums: List[int]) -> Iterable[int]:
        for i, ind in enumerate(indexes):
            el = xor_sums[i] ^ self.xor_sums[ind]
            if self.index(el) == ind:
                yield el
