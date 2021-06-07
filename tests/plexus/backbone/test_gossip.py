from asyncio.queues import Queue
from typing import Iterable

from bami.plexus.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.plexus.backbone.gossip import (
    GossipFrontiersMixin,
    NextPeerSelectionStrategy,
)
from bami.plexus.backbone.payload import BlocksRequestPayload, FrontierPayload
from ipv8.peer import Peer
import pytest

from tests.plexus.mocking.community import MockedCommunity, MockSettings
from tests.plexus.mocking.mock_db import MockChain, MockDBManager


class MockNextPeerSelection(NextPeerSelectionStrategy):
    def get_next_gossip_peers(self, subcom_id: bytes) -> Iterable[Peer]:
        pass


class FakeGossipCommunity(MockedCommunity, GossipFrontiersMixin):
    def incoming_frontier_queue(self, subcom_id: bytes) -> Queue:
        pass

    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        return MockNextPeerSelection()


NUM_NODES = 2


@pytest.fixture
def overlay_class():
    return FakeGossipCommunity


@pytest.fixture
def init_nodes():
    return False


@pytest.fixture
def num_nodes():
    return NUM_NODES


def test_init_correctly(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.decode_map[FrontierPayload.msg_id]
    assert set_vals_by_key.nodes[0].overlay.decode_map[BlocksRequestPayload.msg_id]


@pytest.mark.asyncio
async def test_one_gossip_round(set_vals_by_key, monkeypatch, mocker):
    monkeypatch.setattr(MockDBManager, "get_chain", lambda _, __: MockChain())
    front = Frontier(((1, "val1"),), (), ())
    monkeypatch.setattr(MockChain, "frontier", front)
    monkeypatch.setattr(
        MockNextPeerSelection,
        "get_next_gossip_peers",
        lambda _, subcom, chain_id, frontier, fanout: [
            p.overlay.my_peer for p in set_vals_by_key.nodes
        ],
    )
    monkeypatch.setattr(
        MockDBManager,
        "reconcile",
        lambda _, c_id, frontier, pub_key: FrontierDiff(((1, 1),), {}),
    )
    monkeypatch.setattr(MockSettings, "frontier_gossip_collect_time", 0.1)
    monkeypatch.setattr(MockSettings, "frontier_gossip_fanout", 5)
    monkeypatch.setattr(
        MockDBManager,
        "get_block_blobs_by_frontier_diff",
        lambda _, c_id, f_diff, __: [b"blob1"],
    )

    spy = mocker.spy(set_vals_by_key.nodes[0].overlay, "send_packet")
    set_vals_by_key.nodes[0].overlay.frontier_gossip_sync_task(
        set_vals_by_key.community_id
    )
    spy.assert_called()
