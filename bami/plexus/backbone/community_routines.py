from abc import ABC, abstractmethod
from typing import Callable, Type

from bami.plexus.backbone.datastore.database import BaseDB
from bami.plexus.backbone.settings import BamiSettings
from ipv8.keyvault.keys import Key
from ipv8.messaging.payload import Payload
from ipv8.peer import Peer


class CommunityRoutines(ABC):
    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    @abstractmethod
    def send_packet(self, peer: Peer, packet: Payload, sig: bool = True) -> None:
        """Send packet payload to the peer"""
        pass

    @property
    @abstractmethod
    def persistence(self) -> BaseDB:
        pass

    @property
    @abstractmethod
    def settings(self) -> BamiSettings:
        pass


class MessageStateMachine(ABC):
    @abstractmethod
    def add_message_handler(self, msg_id: Type[Payload], handler: Callable) -> None:
        pass

    @abstractmethod
    def setup_messages(self) -> None:
        pass
