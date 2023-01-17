from ipv8.messaging.payload_dataclass import dataclass
from ipv8.messaging.serialization import default_serializer

from bami.lz.sketch.iblt import IBF, IBLT, reconcile_ibfs
from bami.lz.utils import uhash


@dataclass
class IBLTCell:
    id_sum: int
    item: bytes
    count: int


@dataclass
class IBLTPayload:
    t: [IBLTCell]


def test_iblt_pack():
    table = IBLT()
    b = table.generate_table([i for i in range(100)])
    payload = IBLTPayload([IBLTCell(v[0], int.to_bytes(v[1], 16, 'big'), v[2]) for v in b])

    b2 = table.generate_table([1])
    v, v2, l = table.compare_tables(b, b2)
    pack = default_serializer.pack_serializable(payload)
    unpacked_payload: IBLTPayload = default_serializer.unpack_serializable(IBLTPayload, pack)[0]

    assert unpacked_payload.t == payload.t


def test_iblt_reconciliation():
    m = 101

    b1 = IBLT(m)
    b2 = IBLT(m)

    t1 = b1.generate_table([uhash(str(i)) for i in range(10, 100)])
    t2 = b1.generate_table([uhash(str(i)) for i in range(1, 10)])

    print(t1)
    print("--", t2)

    print(b1.compare_tables(t1, t2))
    print(b1.compare_tables(t2, t1))


def test_ibf_reconciliation():
    m = 100
    b1 = IBF(m)
    b2 = IBF(m)
    all_vals = set()

    for i in range(10, 100):
        all_vals.
        b1.add(uhash(str(i)))
    for i in range(1, 10):
        b2.add(uhash(str(i)))
    print(reconcile_ibfs(b1, b2))
    print(reconcile_ibfs(b2, b1))

    print(reconcile_ibfs(b1, b2))

