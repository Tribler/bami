from typing import Any, Optional, Union, Type, Iterable

from ipv8.keyvault.keys import Key
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from python_project.backbone.community_routines import CommunityRoutines
from python_project.backbone.datastore.database import BaseDB
from python_project.backbone.sub_community import (
    SubCommunityRoutines,
    BaseSubCommunityFactory,
    SubCommunityDiscoveryStrategy,
    BaseSubCommunity,
)

from tests.mocking.mock_db import MockDBManager


class FakeRoutines(CommunityRoutines):
    @property
    def network(self) -> Network:
        pass

    @property
    def my_peer(self) -> Peer:
        return Peer(self.key)

    @property
    def logger(self) -> Any:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        pass

    def __init__(self):
        self.key = self.crypto.generate_key(u"medium")

    @property
    def my_peer_key(self) -> Key:
        return self.key

    @property
    def my_pub_key(self) -> bytes:
        return self.key.pub().key_to_bin()

    def send_packet(self, peer: Peer, packet: Any) -> None:
        pass

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()


class MockSubCommuntiy(BaseSubCommunity):
    @property
    def subcom_id(self) -> bytes:
        pass

    def get_known_peers(self) -> Iterable[Peer]:
        pass


class MockSubCommunityDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    def discover(self, subcom: BaseSubCommunity) -> None:
        pass


class MockSubCommunityFactory(BaseSubCommunityFactory):
    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        return MockSubCommuntiy()


class MockSubCommunityRoutines(SubCommunityRoutines):
    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    @property
    def my_subcoms(self) -> Iterable[bytes]:
        pass

    def add_subcom(self, sub_com: bytes, subcom_obj: BaseSubCommunity) -> None:
        pass

    def notify_peer_on_new_subcoms(self) -> None:
        pass

    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        pass

    def get_subcom_discovery_strategy(
        self, subcom_id: bytes
    ) -> Union[SubCommunityDiscoveryStrategy, Type[SubCommunityDiscoveryStrategy]]:
        return MockSubCommunityDiscoveryStrategy()

    @property
    def subcom_factory(
        self,
    ) -> Union[BaseSubCommunityFactory, Type[BaseSubCommunityFactory]]:
        return MockSubCommunityFactory()
