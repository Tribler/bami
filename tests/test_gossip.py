from typing import Iterable

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
from ipv8.test.mocking.endpoint import internet
import pytest
from python_project.backbone.datastore.frontiers import Frontier, FrontierDiff
from python_project.backbone.gossip import (
    GossipFrontiersMixin,
    NextPeerSelectionStrategy,
)
from python_project.backbone.payload import BlocksRequestPayload, FrontierPayload

from tests.mocking.base import TestBase
from tests.mocking.community import MockedCommunity, MockSettings
from tests.mocking.mock_db import MockChain, MockDBManager


class MockNextPeerSelection(NextPeerSelectionStrategy):
    def get_next_gossip_peers(self, subcom_id: bytes) -> Iterable[Peer]:
        pass


class FakeGossipCommunity(MockedCommunity, GossipFrontiersMixin):
    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        return MockNextPeerSelection()


class TestGossipBase(TestBase):
    __testing__ = False
    NUM_NODES = 2

    @pytest.yield_fixture(autouse=True)
    def main_fix(self):
        self.nodes = []
        internet.clear()
        self._tempdirs = []

        super().setUp()
        self.initialize(FakeGossipCommunity, self.NUM_NODES)

        # TODO: Add additional setup
        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        # for node in self.nodes:
        #    node.overlay.subscribe_to_subcom(self.community_id)
        yield

    @pytest.mark.asyncio
    async def test_init_correctly(self):
        assert chr(FrontierPayload.msg_id) in self.nodes[0].overlay.decode_map
        assert chr(BlocksRequestPayload.msg_id) in self.nodes[0].overlay.decode_map
        await self.tearDown()

    @pytest.mark.asyncio
    async def test_one_gossip_round(self, monkeypatch, mocker):
        monkeypatch.setattr(MockDBManager, "get_chain", lambda _, __: MockChain())
        front = Frontier(((1, "val1"),), (), ())
        monkeypatch.setattr(MockChain, "frontier", front)
        monkeypatch.setattr(
            MockNextPeerSelection,
            "get_next_gossip_peers",
            lambda _, __, ___: [p.overlay.my_peer for p in self.nodes],
        )
        monkeypatch.setattr(
            MockDBManager,
            "reconcile",
            lambda _, c_id, frontier, pub_key: FrontierDiff(((1, 1),), {}),
        )
        monkeypatch.setattr(MockSettings, "gossip_collect_time", 0.1)
        monkeypatch.setattr(MockSettings, "gossip_fanout", 5)
        monkeypatch.setattr(
            MockDBManager,
            "get_block_blobs_by_frontier_diff",
            lambda _, c_id, f_diff, __: [b"blob1"],
        )

        self.nodes[0].overlay.gossip_sync_task(self.community_id)
        spy = mocker.spy(self.nodes[0].overlay, "send_packet")

        await self.deliver_messages(0.5)
        spy.assert_called()
        await self.tearDown()
