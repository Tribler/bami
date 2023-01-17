from pyroaring import BitMap

from bami.lz.utils import uhash


def test_bitmap():
    v = BitMap()
    for k in range(1, 100):
        v.add(uhash(k))
    block = BitMap.serialize(v)
