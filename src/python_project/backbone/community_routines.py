from abc import ABC, abstractmethod
from typing import Any, Optional, Type, Callable

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.keys import Key
from ipv8.messaging.payload import Payload
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from python_project.backbone.datastore.database import BaseDB


class CommunityRoutines(ABC):
    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    @property
    @abstractmethod
    def my_peer(self) -> Peer:
        pass

    @abstractmethod
    def send_packet(self, peer: Peer, packet: Payload, sig: bool = True) -> None:
        """Send packet payload to the peer"""
        pass

    @property
    @abstractmethod
    def persistence(self) -> BaseDB:
        pass

    @property
    def crypto(self):
        return default_eccrypto

    @property
    @abstractmethod
    def logger(self) -> Any:
        pass

    @property
    @abstractmethod
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    @abstractmethod
    def settings(self) -> Any:
        pass

    @property
    @abstractmethod
    def network(self) -> Network:
        pass

    @property
    @abstractmethod
    def request_cache(self) -> RequestCache:
        pass


class MessageStateMachine(ABC):
    @abstractmethod
    def add_message_handler(self, msg_id: Type[Payload], handler: Callable) -> None:
        pass

    @abstractmethod
    def setup_messages(self) -> None:
        pass
