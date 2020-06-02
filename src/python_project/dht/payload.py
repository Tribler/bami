from __future__ import annotations

from socket import inet_aton, inet_ntoa
from struct import calcsize, pack, unpack, unpack_from

from python_project.dht.routing import Node
from python_project.messaging.payload import Payload
from python_project.peer import Peer
from typing import List, Tuple, Union


def encode_values(values: List[bytes]) -> bytes:
    return b"".join([pack("!H", len(value)) + value for value in values])


def decode_values(values_str: bytes) -> List[bytes]:
    values = []
    index = 0
    while index < len(values_str):
        length = unpack_from("!H", values_str, offset=index)[0]
        index += calcsize("!H")
        values.append(values_str[index : index + length])
        index += length
    return values


def encode_nodes(nodes: Union[List[Peer], List[Node]]) -> bytes:
    nodes_str = b""
    for node in nodes:
        key = node.public_key.key_to_bin()
        nodes_str += inet_aton(node.address[0]) + pack("!H", node.address[1])
        nodes_str += pack("!H", len(key)) + key
    return nodes_str


def decode_nodes(nodes_str: bytes) -> List[Node]:
    nodes = []
    index = 0
    while index < len(nodes_str):
        ip, port, key_length = unpack("!4sHH", nodes_str[index : index + 8])
        index += 8
        address = (inet_ntoa(ip), port)
        key = nodes_str[index : index + key_length]
        index += key_length
        nodes.append(Node(key, address=address))
    return nodes


class BasePayload(Payload):

    format_list = ["I"]

    def __init__(self, identifier: int) -> None:
        super(BasePayload, self).__init__()
        self.identifier = identifier

    def to_pack_list(self) -> List[Tuple[str, int]]:
        return [("I", self.identifier)]

    @classmethod
    def from_unpack_list(cls, identifier: int) -> BasePayload:
        return BasePayload(identifier)


class PingRequestPayload(BasePayload):
    pass


class PingResponsePayload(BasePayload):
    pass


class StoreRequestPayload(BasePayload):

    format_list = BasePayload.format_list + ["20s", "20s", "varlenH"]

    def __init__(
        self, identifier: int, token: bytes, target: bytes, values: List[bytes]
    ) -> None:
        super(StoreRequestPayload, self).__init__(identifier)
        self.token = token
        self.target = target
        self.values = values

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = super(StoreRequestPayload, self).to_pack_list()
        data.append(("20s", self.token))
        data.append(("20s", self.target))
        data.append(("varlenH", encode_values(self.values)))
        return data

    @classmethod
    def from_unpack_list(
        cls, identifier: int, token: bytes, target: bytes, values_str: bytes
    ) -> StoreRequestPayload:
        values = decode_values(values_str)
        return StoreRequestPayload(identifier, token, target, values)


class StoreResponsePayload(BasePayload):
    pass


class FindRequestPayload(BasePayload):

    format_list = BasePayload.format_list + ["varlenI", "20s", "I", "?"]

    def __init__(
        self,
        identifier: int,
        lan_address: Tuple[str, int],
        target: bytes,
        start_idx: int,
        force_nodes: bool,
    ) -> None:
        super(FindRequestPayload, self).__init__(identifier)
        self.lan_address = lan_address
        self.target = target
        self.start_idx = start_idx
        self.force_nodes = force_nodes

    def to_pack_list(
        self,
    ) -> List[Union[Tuple[str, int], Tuple[str, bytes], Tuple[str, bool]]]:
        data = super(FindRequestPayload, self).to_pack_list()
        data.append(
            (
                "varlenI",
                inet_aton(self.lan_address[0]) + pack("!H", self.lan_address[1]),
            )
        )
        data.append(("20s", self.target))
        data.append(("I", self.start_idx))
        data.append(("?", self.force_nodes))
        return data

    @classmethod
    def from_unpack_list(
        cls,
        identifier: int,
        lan_address: bytes,
        target: bytes,
        start_idx: int,
        force_nodes: bool,
    ) -> FindRequestPayload:
        return FindRequestPayload(
            identifier,
            (inet_ntoa(lan_address[:4]), unpack("!H", lan_address[4:6])[0]),
            target,
            start_idx,
            force_nodes,
        )


class FindResponsePayload(BasePayload):

    format_list = BasePayload.format_list + ["20s", "varlenH", "varlenH"]

    def __init__(
        self, identifier: int, token: bytes, values: List[bytes], nodes: List[Node]
    ) -> None:
        super(FindResponsePayload, self).__init__(identifier)
        self.token = token
        self.values = values
        self.nodes = nodes

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = super(FindResponsePayload, self).to_pack_list()
        data.append(("20s", self.token))
        data.append(("varlenH", encode_values(self.values)))
        data.append(("varlenH", encode_nodes(self.nodes)))
        return data

    @classmethod
    def from_unpack_list(
        cls, identifier: int, token: bytes, values_str: bytes, nodes_str: bytes
    ) -> FindResponsePayload:
        return FindResponsePayload(
            identifier, token, decode_values(values_str), decode_nodes(nodes_str)
        )


class StrPayload(Payload):

    format_list = ["raw"]

    def __init__(self, data: bytes) -> None:
        super(StrPayload, self).__init__()
        self.data = data

    def to_pack_list(self) -> List[Tuple[str, bytes]]:
        return [("raw", self.data)]

    @classmethod
    def from_unpack_list(cls, data: bytes) -> StrPayload:
        return StrPayload(data)


class SignedStrPayload(Payload):

    format_list = ["varlenH", "I", "varlenH"]

    def __init__(self, data: bytes, version: int, public_key: bytes) -> None:
        super(SignedStrPayload, self).__init__()
        self.data = data
        self.version = version
        self.public_key = public_key

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        return [
            ("varlenH", self.data),
            ("I", self.version),
            ("varlenH", self.public_key),
        ]

    @classmethod
    def from_unpack_list(
        cls, data: bytes, version: int, public_key: bytes
    ) -> SignedStrPayload:
        return SignedStrPayload(data, version, public_key)


class StorePeerRequestPayload(BasePayload):

    format_list = BasePayload.format_list + ["20s", "20s"]

    def __init__(self, identifier: int, token: bytes, target: bytes) -> None:
        super(StorePeerRequestPayload, self).__init__(identifier)
        self.token = token
        self.target = target

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = super(StorePeerRequestPayload, self).to_pack_list()
        data.append(("20s", self.token))
        data.append(("20s", self.target))
        return data

    @classmethod
    def from_unpack_list(
        cls, identifier: int, token: bytes, target: bytes
    ) -> StorePeerRequestPayload:
        return StorePeerRequestPayload(identifier, token, target)


class StorePeerResponsePayload(BasePayload):
    pass


class ConnectPeerRequestPayload(BasePayload):

    format_list = BasePayload.format_list + ["varlenI", "20s"]

    def __init__(
        self, identifier: int, lan_address: Tuple[str, int], target: bytes
    ) -> None:
        super(ConnectPeerRequestPayload, self).__init__(identifier)
        self.lan_address = lan_address
        self.target = target

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = super(ConnectPeerRequestPayload, self).to_pack_list()
        data.append(
            (
                "varlenI",
                inet_aton(self.lan_address[0]) + pack("!H", self.lan_address[1]),
            )
        )
        data.append(("20s", self.target))
        return data

    @classmethod
    def from_unpack_list(
        cls, identifier: int, lan_address: bytes, target: bytes
    ) -> ConnectPeerRequestPayload:
        return ConnectPeerRequestPayload(
            identifier,
            (inet_ntoa(lan_address[:4]), unpack("!H", lan_address[4:6])[0]),
            target,
        )


class ConnectPeerResponsePayload(BasePayload):

    format_list = BasePayload.format_list + ["varlenH"]

    def __init__(self, identifier: int, nodes: Union[List[Node], List[Peer]]) -> None:
        super(ConnectPeerResponsePayload, self).__init__(identifier)
        self.nodes = nodes

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = super(ConnectPeerResponsePayload, self).to_pack_list()
        data.append(("varlenH", encode_nodes(self.nodes)))
        return data

    @classmethod
    def from_unpack_list(
        cls, identifier: int, nodes: bytes
    ) -> ConnectPeerResponsePayload:
        return ConnectPeerResponsePayload(identifier, decode_nodes(nodes))
