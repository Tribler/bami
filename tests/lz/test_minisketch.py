from copy import copy, deepcopy

from bami.lz.sketch.minisketch import MiniSketch
from bami.lz.utils import get_random_string, uhash


def cell_id(item_val: int, seed: int, n_cells) -> int:
    """Get cell id associated with the item"""
    return (item_val ^ seed) % n_cells


def test_minisketch():
    N = 10000
    n = 20
    M = 20

    m1_vals = set()

    m = MiniSketch(M)
    for i in range(1, N):
        m.raw_add(i)

    m2 = MiniSketch(M)
    for i in range(1, N - n):
        m2.raw_add(i)

    m3 = MiniSketch(M)
    for i in range(n, N):
        m3.raw_add(i)

    val2 = m2.serialize()
    val3 = m3.serialize()

    m_c = MiniSketch(M)
    m_c.merge(m.serialize())
    m_c.merge(val2)
    els = m_c.decode()
    print(els)

    m_c = MiniSketch(M)
    m_c.merge(m.serialize())
    m_c.merge(val3)
    els = m_c.decode()
    print(els)


    #other_m = MiniSketch(M)
    #other_m.merge(m)
    #other_m.merge()

    #m_c = copy(m)
    #m_c.merge(val3)
    #els = m_c.decode()
    #print(els)



def test_dif_sketch():
    N = 1000
    M = 110
    nc = 10

    mini1 = MiniSketch(M)
    comp = [MiniSketch(M) for _ in range(nc)]
    for i in range(0, N):
        val = uhash(str(i))
        c = cell_id(val, 123, nc)
        mini1.raw_add(val)
        comp[c].raw_add(val)

    print(comp[0].decode())

    mini1.merge(comp[0].serialize())
    print(mini1.decode())


def test_mini_dublicates():
    m1 = MiniSketch(10)
    m2 = MiniSketch(10)

    m1.raw_add(1)
    m1.raw_add(1)
    m1.raw_add(1)

    m2.raw_add(1)

    m1.merge(m2.serialize())
    print(m1.decode())

def test_composite_sketch():
    N = 10000
    n = 100
    M = 10

    nc = 10

    minis = [MiniSketch(M) for _ in range(nc)]
    one_mini = MiniSketch(nc * M)
    for i in range(1, N - n):
        val = uhash(str(i))
        c = cell_id(val, 123, nc)
        minis[c].raw_add(val)
        if c < 10:
            one_mini.raw_add(val)

    minis2 = [MiniSketch(M) for _ in range(nc)]
    one_mini_2 = MiniSketch(10 * M)
    for i in range(1, N):
        val = uhash(str(i))
        c = cell_id(val, 123, nc)
        minis2[c].raw_add(val)
        if c < 10:
            one_mini_2.raw_add(val)

    total_rec = 0
    for k in range(10):
        try:
            minis[k].merge(minis2[k].serialize())
            vals = minis[k].decode()
            total_rec += len(vals)
        except IndexError as e:
            pass

    one_mini.merge(one_mini_2.serialize())
    vals = one_mini.decode()
    print(len(vals), "vs ", total_rec)
