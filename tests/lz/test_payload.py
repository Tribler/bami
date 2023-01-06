from ipv8.messaging.payload_dataclass import dataclass
from ipv8.messaging.serialization import default_serializer

from bami.lz.payload import CompactSketch, ReconciliationRequestPayload
from bami.lz.sketch.peer_clock import dummy_clock


def test_compact_sketch():
    c = CompactSketch(seed=10, data=b"0101", csum=b'010')
    pack = default_serializer.pack_serializable(c)
    unpacked_payload: CompactSketch = default_serializer.unpack_serializable(CompactSketch, pack)[0]
    assert unpacked_payload.seed == 10
    assert unpacked_payload.data == b'0101'


def test_challenge_pack():
    sketch = CompactSketch(seed=10, data=b'0101', csum=b'123')
    clock = dummy_clock
    pscp = ReconciliationRequestPayload(clock=clock, sketch=sketch)

    pack = default_serializer.pack_serializable(pscp)
    unpacked_payload: ReconciliationRequestPayload = \
        default_serializer.unpack_serializable(ReconciliationRequestPayload,
                                               pack)[0]
    assert unpacked_payload.sketch.data == b'0101'
    assert unpacked_payload.clock.clock == b'0'


@dataclass
class TP1:
    sketch: CompactSketch
    sketch2: CompactSketch


@dataclass
class TP2:
    sketch: [CompactSketch]


def test_packing_sketches():
    sketch = CompactSketch(seed=10, data=b'0101', csum=b'10345')

    t1 = TP1(sketch, sketch)
    t2 = TP2([sketch, sketch])

    pack1 = default_serializer.pack_serializable(t1)
    pack2 = default_serializer.pack_serializable(t2)

    print(len(pack1), len(pack2))
