from bami.lz.sketch.pbs import CMS, PBS
from bami.lz.utils import get_random_string, uhash


def test_cms():
    v = CMS(10, 6)
    v2 = CMS(10, 6)

    vals = [uhash(get_random_string(10)) for i in range(1, 10)]
    raw_vals = []
    vals2 = [uhash(get_random_string(10)) for i in range(1, 50, 1)]
    raw_vals2 = []
    for i in vals:
        v.add(i)
    for j in vals2:
        v2.add(j)

    serial = v.serialize()
    new_vals = v2.reconcile(serial)
    s = 0
    for k in new_vals:
        if k:
            s += len(k)
            assert set(k).issubset(set(vals) | set(vals2))
    print(s)


def test_pbs():
    v = PBS(40, 80, seed=2)
    v2 = PBS(40, 80, seed=2)

    vals = [i for i in range(1, 10)]
    vals2 = [i for i in range(1, 100, 1)]
    for i in vals:
        v.add(i)

    for j in vals2:
        v2.add(j)

    print(v.checksum, v2.checksum)

    ind = v.identify(v2.mini.serialize())
    xors = [v.xor_sums[i - 1] for i in ind]
    for k in v2.reconcile([i - 1 for i in ind], xors):
        print(k)

    print()
