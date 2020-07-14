from typing import Any, Optional, Union, Type, Iterable, Dict

from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.keys import Key
from ipv8.messaging.payload import Payload
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from python_project.backbone.block import PlexusBlock
from python_project.backbone.community import PlexusCommunity, BlockResponse
from python_project.backbone.community_routines import CommunityRoutines, MessageStateMachine
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
    def request_cache(self) -> RequestCache:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        pass

    def __init__(self):
        self.crypto = default_eccrypto
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

    def notify_peers_on_new_subcoms(self) -> None:
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


class MockSettings(object):

    @property
    def sync_timeout(self):
        return 0.1


class MockedCommunity(Community, CommunityRoutines):
    master_peer = Peer(default_eccrypto.generate_key(u"very-low"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._req = RequestCache()

        for base in self.__class__.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()

    @property
    def settings(self) -> Any:
        return MockSettings()

    def send_packet(self, *args, **kwargs) -> None:
        self.ez_send(*args, **kwargs)

    @property
    def request_cache(self) -> RequestCache:
        return self._req

    async def unload(self):
        await self._req.shutdown()
        return await super().unload()


class FakeBackCommunity(PlexusCommunity):
    def witness_tx_well_formatted(self, witness_tx: Any) -> bool:
        pass

    def build_witness_blob(self, chain_id: bytes, seq_num: int) -> Optional[bytes]:
        pass

    def apply_witness_tx(self, block: PlexusBlock, witness_tx: Any) -> None:
        pass

    def apply_confirm_tx(self, block: PlexusBlock, confirm_tx: Dict) -> None:
        pass

    def apply_reject_tx(self, block: PlexusBlock, reject_tx: Dict) -> None:
        pass

    def block_response(
            self, block: PlexusBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        pass

    def process_block_out_of_order(self, blk: PlexusBlock, peer: Peer) -> None:
        pass

    def notify_peers_on_new_subcoms(self) -> None:
        pass

    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        pass

    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        pass

    def discover(
            self, subcom: BaseSubCommunity, target_peers: int = -1, **kwargs
    ) -> None:
        pass
