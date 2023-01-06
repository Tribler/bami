import numpy as np

from bami.lz.sketch.bloom import BloomFilter


def test_bloom_filter():
    m = 8*100
    nf = 2

    b = BloomFilter(m, num_func=nf)
    for i in range(1, 100):
        b.add(i)
    assert len(b.to_bytes()) == m/8
