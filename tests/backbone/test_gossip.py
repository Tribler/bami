from asyncio.queues import Queue
from typing import Iterable

from bami.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.backbone.gossip import (
    GossipFrontiersMixin,
    NextPeerSelectionStrategy,
)
from bami.backbone.payload import BlocksRequestPayload, FrontierPayload
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
import pytest

from tests.mocking.base import (
    create_and_connect_nodes,
    SetupValues,
    unload_nodes,
)
from tests.mocking.community import MockedCommunity, MockSettings
from tests.mocking.mock_db import MockChain, MockDBManager


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


@pytest.fixture()
def overlay_class():
    return FakeGossipCommunity


@pytest.fixture()
async def set_vals(tmpdir_factory, overlay_class):
    dirs = [
        tmpdir_factory.mktemp(str(overlay_class.__name__), numbered=True)
        for _ in range(NUM_NODES)
    ]
    nodes = create_and_connect_nodes(NUM_NODES, work_dirs=dirs, ov_class=overlay_class)
    # Make sure every node has a community to listen to
    community_key = default_eccrypto.generate_key(u"curve25519").pub()
    community_id = community_key.key_to_bin()
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    for k in dirs:
        k.remove(ignore_errors=True)


def test_init_correctly(set_vals):
    assert set_vals.nodes[0].overlay.decode_map[FrontierPayload.msg_id]
    assert set_vals.nodes[0].overlay.decode_map[BlocksRequestPayload.msg_id]


@pytest.mark.asyncio
async def test_one_gossip_round(set_vals, monkeypatch, mocker):
    monkeypatch.setattr(MockDBManager, "get_chain", lambda _, __: MockChain())
    front = Frontier(((1, "val1"),), (), ())
    monkeypatch.setattr(MockChain, "frontier", front)
    monkeypatch.setattr(
        MockNextPeerSelection,
        "get_next_gossip_peers",
        lambda _, subcom, chain_id, frontier, fanout: [
            p.overlay.my_peer for p in set_vals.nodes
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

    spy = mocker.spy(set_vals.nodes[0].overlay, "send_packet")
    set_vals.nodes[0].overlay.frontier_gossip_sync_task(set_vals.community_id)
    spy.assert_called()
