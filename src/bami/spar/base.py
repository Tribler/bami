from abc import abstractmethod
from typing import Optional, Tuple, Type

from ipv8.community import Community
from ipv8.lazy_community import PacketDecodingError
from ipv8.messaging.payload_headers import BinMemberAuthenticationPayload
from ipv8.types import Address, Payload, Peer

from bami.lz.database.database import TransactionSyncDB


class BaseMixin:

    @property
    @abstractmethod
    def settings(self):
        pass

    @property
    @abstractmethod
    def db(self) -> TransactionSyncDB:
        pass

    @property
    @abstractmethod
    def my_peer_id(self) -> bytes:
        pass

    @abstractmethod
    def get_peers(self):
        pass

    @abstractmethod
    def register_task(self, *args, **kwargs):
        pass

    @abstractmethod
    def ez_send(self, *args, **kwargs) -> None:
        pass


class BaseCommunity(Community):

    def prepare_packet(self, payload: Payload, sig: bool = True) -> bytes:
        return self.ezr_pack(payload.msg_id, payload, sig=sig)

    def pack_payload(self, serializable: Payload) -> bytes:
        return self.serializer.pack_serializable(serializable)

    def unpack_payload(self, payload_class: Type[Payload], data: bytes) -> Payload:
        return self.serializer.unpack_serializable(payload_class, data)[0]

    def send_packet(self, address: Address, packet: bytes) -> None:
        self.endpoint.send(address, packet)

    def send_payload(self, peer: Peer, payload: Payload, sig: bool = True) -> None:
        self.ez_send(peer, payload, sig=sig)

    def parse_raw_packet(
        self,
        packet: bytes,
        payload_class: Type[Payload],
        source_address: Optional[Address] = None,
    ) -> Tuple[Peer, Payload]:
        """Unpack and verify the payload_cls"""

        auth, _ = self.serializer.unpack_serializable(
            BinMemberAuthenticationPayload, packet, offset=23
        )
        signature_valid, remainder = self._verify_signature(auth, packet)
        unpacked_payload, _ = self.serializer.unpack_serializable(
            payload_class, remainder, offset=23
        )
        # ASSERT
        if not signature_valid:
            raise PacketDecodingError(
                "Incoming packet %s has an invalid signature"
                % str(payload_class.__name__)
            )
        # PRODUCE
        peer = self.network.verified_by_public_key_bin.get(auth.public_key_bin) or Peer(
            auth.public_key_bin, source_address
        )
        return peer, unpacked_payload
