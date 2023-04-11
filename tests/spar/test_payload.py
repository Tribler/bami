from dataclasses import dataclass
from ipv8.messaging.payload_dataclass import overwrite_dataclass
from ipv8.messaging.serialization import default_serializer

dataclass = overwrite_dataclass(dataclass)


@dataclass()
class T:
    t: bytes


@dataclass(msg_id=10)
class VP:
    t: [T]


def test_payload_data():
    v = VP(t=[T(b'1'), T(b'2'), T(b'3')])
    print(v)
    pack1 = default_serializer.pack_serializable(v)
    pack2 = default_serializer.unpack_serializable(VP, pack1)
    assert v == pack2[0]
