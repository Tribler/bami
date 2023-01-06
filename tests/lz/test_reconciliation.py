from typing import List

from bami.lz.sketch.bloom import BloomFilter
from bami.lz.sketch.peer_clock import PeerClock
from bami.lz.reconcile import CompactReconciliationSet
from bami.lz.utils import *


def reconcile_composite(pool1: List[CompactReconciliationSet], pool2: List[CompactReconciliationSet]):
    # Reconcile for each of the cell
    all_vals = set()
    m = len(pool1)
    for i in range(m):
        all_vals.update(pool1[i].reconcile(pool2[i].sketch))
    return all_vals


def test_composite_reconciliation():
    """Exchange transactions  between two peers"""
    m = 64
    bm = 32
    n = 10000
    delta = 10

    data1 = [uhash(get_random_string(10)) for _ in range(n)]
    data2 = data1[:n - delta]
    data2.extend([uhash(get_random_string(10)) for _ in range(delta)])

    clock1 = PeerClock(m)
    clock2 = PeerClock(m)

    all_vals = set()
    next_size = set()
    def size(i, ns): return bm*4 if i in ns else bm
    for k in range(10):
        f_id = uhash('peer1') ^ uhash('peer2') ^ k

        pool1 = [CompactReconciliationSet(BloomFilter(size(i, next_size), num_func=1, seed_value=f_id ^ uhash(i)))
                 for i in range(m)]
        pool2 = [CompactReconciliationSet(BloomFilter(size(i, next_size), num_func=1, seed_value=f_id ^ uhash(i)))
                 for i in range(m)]

        for i in data1:
            j = clock1.increment(i)
            pool1[j].add_transaction(i)
        for i in data2:
            j = clock2.increment(i)
            pool2[j].add_transaction(i)

        diff_vals = reconcile_composite(pool1, pool2)
        all_vals.update(diff_vals)
        print(len(diff_vals), len(all_vals))

        next_size = set()
        for i in range(len(pool1)):
            if pool1[i].sketch.checksum != pool2[i].sketch.checksum:
                next_size.add(i)
        print(len(next_size))


def test_bloom_reconciliation():
    """Exchange transactions  between two peers"""
    m = 16
    n = 6000
    delta = 10

    data1 = [uhash(get_random_string(10)) for _ in range(n)]
    data2 = data1[:n - delta]
    data2.extend([uhash(get_random_string(10)) for _ in range(delta)])

    all_vals = set()
    for k in range(10):
        f_id = uhash('peer1') ^ uhash('peer2') ^ k
        bm = 2048
        bloom1 = BloomFilter(bm, num_func=1, seed_value=f_id)
        pool1 = CompactReconciliationSet(bloom1)

        bloom2 = BloomFilter(bm, num_func=1, seed_value=f_id)
        pool2 = CompactReconciliationSet(bloom2)
        # Prepare transactions for each of the peers
        for i in data1:
            pool1.add_transaction(i)
        for i in data2:
            pool2.add_transaction(i)

        # Reconciliate between peer1 and peer2
        diff_vals = pool1.reconcile(pool2.sketch)
        all_vals.update(diff_vals)
        print(len(diff_vals), len(all_vals))
