from typing import Any, Optional

from bami.backbone.datastore.database import BaseDB
from bami.backbone.discovery import SubCommunityDiscoveryStrategy
from bami.backbone.mixins import MixinRoutines
from bami.backbone.settings import BamiSettings
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8_service import IPv8
import pytest

from tests.mocking.base import create_node
from tests.mocking.community import FakeBackCommunity


class FakeMixin(MixinRoutines):
    def setup_mixin(self) -> None:
        self.inited = True

    def unload_mixin(self) -> None:
        self.unloaded = True


class MixinedBackCommunity(FakeBackCommunity, FakeMixin):
    def __init__(
        self,
        my_peer: Peer,
        endpoint: Any,
        network: Network,
        ipv8: Optional[IPv8] = None,
        max_peers: int = None,
        anonymize: bool = False,
        db: BaseDB = None,
        work_dir: str = None,
        discovery_strategy: SubCommunityDiscoveryStrategy = None,
        settings: BamiSettings = None,
        **kwargs
    ):
        self.inited = False
        self.unloaded = False
        super().__init__(
            my_peer,
            endpoint,
            network,
            ipv8,
            max_peers,
            anonymize,
            db,
            work_dir,
            discovery_strategy,
            settings,
            **kwargs
        )


@pytest.mark.asyncio
async def test_mixin_logic(tmpdir_factory):
    dir = tmpdir_factory.mktemp(str(MixinedBackCommunity.__name__), numbered=True)
    node = create_node(MixinedBackCommunity, work_dir=str(dir))
    assert node.overlay.inited
    await node.unload()
    assert node.overlay.unloaded
    dir.remove(ignore_errors=True)
