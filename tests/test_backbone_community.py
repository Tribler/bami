from collections import namedtuple
from unittest.mock import ANY

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.test.mocking.endpoint import internet
import pytest
from python_project.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)

from tests.conftest import FakeBlock
from tests.mocking.base import TestBase
from tests.mocking.community import (
    FakeIPv8BackCommunity,
    FakeLightBackCommunity,
)


class TestBackBoneIPv8Community(TestBase):
    __testing__ = False
    NUM_NODES = 2

    def setup_nodes(self, light_subcoms: bool = False):
        self.nodes = []
        internet.clear()
        self._tempdirs = []
        self.setUp()
        subcom = FakeIPv8BackCommunity if not light_subcoms else FakeLightBackCommunity
        self.initialize(subcom, self.NUM_NODES)

        # TODO: Add additional setup
        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        for node in self.nodes:
            node.overlay.subscribe_to_subcom(self.community_id)

    @pytest.fixture()
    async def main_fix(self):
        SetupVals = namedtuple("SetupVals", ["nodes", "community_id"])
        self.setup_nodes()
        yield SetupVals(self.nodes, self.community_id)
        await self.tearDown()

    # TestIntroduction

    @pytest.mark.asyncio
    async def test_share_in_community(self, mocker, main_fix):
        blk = FakeBlock(com_id=main_fix.community_id)
        main_fix.nodes[0].overlay.share_in_community(blk, main_fix.community_id)
        spy = mocker.spy(main_fix.nodes[1].overlay, "validate_persist_block")
        await self.deliver_messages()
        spy.assert_called_once_with(blk, main_fix.nodes[0].overlay.my_peer.address)

    @pytest.mark.asyncio
    async def test_confirm_block(self, mocker, main_fix):
        blk = FakeBlock(com_id=main_fix.community_id)
        main_fix.nodes[0].overlay.confirm(blk)
        spy = mocker.spy(main_fix.nodes[1].overlay, "validate_persist_block")
        await self.deliver_messages()
        spy.assert_called_with(ANY, main_fix.nodes[0].overlay.my_peer.address)

    @pytest.mark.asyncio
    async def test_reject_block(self, mocker, main_fix):
        blk = FakeBlock(com_id=main_fix.community_id)
        main_fix.nodes[0].overlay.reject(blk)
        spy = mocker.spy(main_fix.nodes[1].overlay, "validate_persist_block")
        await self.deliver_messages()
        spy.assert_called_with(ANY, main_fix.nodes[0].overlay.my_peer.address)

    def test_init_setup(self, main_fix):
        assert (
            chr(RawBlockBroadcastPayload.msg_id) in main_fix.nodes[0].overlay.decode_map
        )
        assert chr(BlockBroadcastPayload.msg_id) in main_fix.nodes[0].overlay.decode_map

    def test_subscribe(self, main_fix):
        assert main_fix.nodes[0].overlay.is_subscribed(main_fix.community_id)
        assert main_fix.nodes[1].overlay.is_subscribed(main_fix.community_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("light_com", [True, False], ids=["light", "ipv8"])
    async def test_peers_introduction(self, mocker, light_com):
        self.setup_nodes(light_com)
        spy = mocker.spy(self.nodes[1].overlay, "process_peer_subscriptions")
        await self.introduce_nodes()
        spy.assert_called()
        for i in range(self.NUM_NODES):
            assert len(self.nodes[i].overlay.my_subcoms) == 1
            assert self.nodes[i].overlay.get_subcom(self.community_id) is not None
            assert (
                len(
                    self.nodes[i]
                    .overlay.get_subcom(self.community_id)
                    .get_known_peers()
                )
                > 0
            )
        await self.tearDown()


# TODO: Test subscribe multiple communities
