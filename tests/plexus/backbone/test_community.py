from unittest.mock import ANY

from bami.plexus.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
import pytest

from tests.plexus.conftest import FakeBlock
from tests.plexus.mocking.base import (
    deliver_messages,
    introduce_nodes,
)
from tests.plexus.mocking.community import (
    FakeIPv8BackCommunity,
    FakeLightBackCommunity,
)

NUM_NODES = 2


@pytest.fixture(params=[FakeIPv8BackCommunity, FakeLightBackCommunity])
def overlay_class(request):
    return request.param


@pytest.fixture
def init_nodes():
    return True


@pytest.fixture
def num_nodes():
    return NUM_NODES


@pytest.mark.asyncio
async def test_share_in_community(mocker, set_vals_by_key):
    blk = FakeBlock(com_id=set_vals_by_key.community_id)
    set_vals_by_key.nodes[0].overlay.share_in_community(
        blk, set_vals_by_key.community_id
    )
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_once_with(blk, set_vals_by_key.nodes[0].overlay.my_peer)


@pytest.mark.asyncio
async def test_confirm_block(mocker, set_vals_by_key):
    blk = FakeBlock(com_id=set_vals_by_key.community_id)
    set_vals_by_key.nodes[0].overlay.confirm(blk)
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_with(ANY, set_vals_by_key.nodes[0].overlay.my_peer)


@pytest.mark.asyncio
async def test_reject_block(mocker, set_vals_by_key):
    blk = FakeBlock(com_id=set_vals_by_key.community_id)
    set_vals_by_key.nodes[0].overlay.reject(blk)
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_with(ANY, set_vals_by_key.nodes[0].overlay.my_peer)


def test_init_setup(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.decode_map[RawBlockBroadcastPayload.msg_id]
    assert set_vals_by_key.nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]


def test_subscribe(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.is_subscribed(set_vals_by_key.community_id)
    assert set_vals_by_key.nodes[1].overlay.is_subscribed(set_vals_by_key.community_id)


@pytest.mark.asyncio
async def test_peers_introduction(mocker, set_vals_by_key):
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "process_peer_subscriptions")
    await introduce_nodes(set_vals_by_key.nodes)
    spy.assert_called()
    for i in range(NUM_NODES):
        assert len(set_vals_by_key.nodes[i].overlay.my_subcoms) == 1
        assert (
            set_vals_by_key.nodes[i].overlay.get_subcom(set_vals_by_key.community_id)
            is not None
        )
        assert (
            len(
                set_vals_by_key.nodes[i]
                .overlay.get_subcom(set_vals_by_key.community_id)
                .get_known_peers()
            )
            > 0
        )


# TODO: Test subscribe multiple communities
