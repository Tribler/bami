from asyncio.queues import Queue
from typing import Any, Dict, Iterable, Optional, Type, Union

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.blockresponse import BlockResponseMixin, BlockResponse
from bami.plexus.backbone.community import PlexusCommunity
from bami.plexus.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.plexus.backbone.datastore.database import BaseDB
from bami.plexus.backbone.discovery import SubCommunityDiscoveryStrategy
from bami.plexus.backbone.settings import BamiSettings
from bami.plexus.backbone.sub_community import (
    BaseSubCommunity,
    BaseSubCommunityFactory,
    IPv8SubCommunityFactory,
    LightSubCommunityFactory,
    SubCommunityRoutines,
)
from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.keys import Key
from ipv8.peer import Peer
from ipv8.requestcache import RequestCache

from tests.plexus.mocking.mock_db import MockDBManager


class FakeRoutines(CommunityRoutines):
    @property
    def request_cache(self) -> RequestCache:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        return BamiSettings()

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
    async def unload(self):
        pass

    def add_peer(self, peer: Peer):
        pass

    @property
    def subcom_id(self) -> bytes:
        pass

    def get_known_peers(self) -> Iterable[Peer]:
        pass


class MockSubCommunityDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    def discover(
        self,
        subcom: BaseSubCommunity,
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        pass


class MockSubCommunityFactory(BaseSubCommunityFactory):
    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        return MockSubCommuntiy()


class MockSubCommunityRoutines(SubCommunityRoutines):
    def discovered_peers_by_subcom(self, subcom_id) -> Iterable[Peer]:
        pass

    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    @property
    def my_subcoms(self) -> Iterable[bytes]:
        pass

    def add_subcom(self, sub_com: bytes, subcom_obj: BaseSubCommunity) -> None:
        pass

    def notify_peers_on_new_subcoms(self) -> None:
        pass

    def on_join_subcommunity(self, sub_com_id: bytes) -> None:
        pass

    @property
    def subcom_factory(
        self,
    ) -> Union[BaseSubCommunityFactory, Type[BaseSubCommunityFactory]]:
        return MockSubCommunityFactory()


class MockSettings(object):
    @property
    def frontier_gossip_collect_time(self):
        return 0.2

    @property
    def frontier_gossip_fanout(self):
        return 5


class MockedCommunity(Community, CommunityRoutines):
    community_id = Peer(default_eccrypto.generate_key(u"very-low")).mid

    def __init__(self, *args, **kwargs):
        if kwargs.get("work_dir"):
            self.work_dir = kwargs.pop("work_dir")
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


class FakeBackCommunity(PlexusCommunity, BlockResponseMixin):
    community_id = b"\x00" * 20

    def incoming_frontier_queue(self, subcom_id: bytes) -> Queue:
        pass

    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        pass

    def apply_confirm_tx(self, block: PlexusBlock, confirm_tx: Dict) -> None:
        pass

    def apply_reject_tx(self, block: PlexusBlock, reject_tx: Dict) -> None:
        pass

    def block_response(
        self, block: PlexusBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        pass

    def process_block_unordered(self, blk: PlexusBlock, peer: Peer) -> None:
        pass

    def received_block_in_order(self, block: PlexusBlock) -> None:
        pass


class FakeIPv8BackCommunity(IPv8SubCommunityFactory, FakeBackCommunity):
    pass


class FakeLightBackCommunity(LightSubCommunityFactory, FakeBackCommunity):
    pass
