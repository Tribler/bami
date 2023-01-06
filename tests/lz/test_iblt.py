from ipv8.messaging.payload_dataclass import dataclass
from ipv8.messaging.serialization import default_serializer

from bami.lz.sketch.iblt import IBF, IBLT, reconcile_ibfs


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


def test_ibf_reconciliation():
    b1 = IBF()
    b2 = IBF()
    for i in range(10, 100):
        b1.add(i)
    for i in range(1, 10):
        b2.add(i)
    v1, v2, b3 = reconcile_ibfs(b1, b2)
