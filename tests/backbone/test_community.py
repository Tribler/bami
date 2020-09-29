from unittest.mock import ANY

from bami.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
from ipv8.keyvault.crypto import default_eccrypto
import pytest

from tests.conftest import FakeBlock
from tests.mocking.base import (
    create_and_connect_nodes,
    deliver_messages,
    introduce_nodes,
    SetupValues,
    unload_nodes,
)
from tests.mocking.community import (
    FakeIPv8BackCommunity,
    FakeLightBackCommunity,
)

NUM_NODES = 2


@pytest.fixture(params=[FakeIPv8BackCommunity, FakeLightBackCommunity])
def overlay_class(request):
    return request.param


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
    for node in nodes:
        node.overlay.subscribe_to_subcom(community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    for k in dirs:
        k.remove(ignore_errors=True)


@pytest.mark.asyncio
async def test_empty_setup(set_vals):
    nodes = set_vals.nodes
    assert len(nodes) == NUM_NODES


@pytest.mark.asyncio
async def test_share_in_community(mocker, set_vals):
    blk = FakeBlock(com_id=set_vals.community_id)
    set_vals.nodes[0].overlay.share_in_community(blk, set_vals.community_id)
    spy = mocker.spy(set_vals.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_once_with(blk, set_vals.nodes[0].overlay.my_peer)


@pytest.mark.asyncio
async def test_confirm_block(mocker, set_vals):
    blk = FakeBlock(com_id=set_vals.community_id)
    set_vals.nodes[0].overlay.confirm(blk)
    spy = mocker.spy(set_vals.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_with(ANY, set_vals.nodes[0].overlay.my_peer)


@pytest.mark.asyncio
async def test_reject_block(mocker, set_vals):
    blk = FakeBlock(com_id=set_vals.community_id)
    set_vals.nodes[0].overlay.reject(blk)
    spy = mocker.spy(set_vals.nodes[1].overlay, "validate_persist_block")
    await deliver_messages()
    spy.assert_called_with(ANY, set_vals.nodes[0].overlay.my_peer)


def test_init_setup(set_vals):
    assert set_vals.nodes[0].overlay.decode_map[RawBlockBroadcastPayload.msg_id]
    assert set_vals.nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]


def test_subscribe(set_vals):
    assert set_vals.nodes[0].overlay.is_subscribed(set_vals.community_id)
    assert set_vals.nodes[1].overlay.is_subscribed(set_vals.community_id)


@pytest.mark.asyncio
async def test_peers_introduction(mocker, set_vals):
    spy = mocker.spy(set_vals.nodes[1].overlay, "process_peer_subscriptions")
    await introduce_nodes(set_vals.nodes)
    spy.assert_called()
    for i in range(NUM_NODES):
        assert len(set_vals.nodes[i].overlay.my_subcoms) == 1
        assert set_vals.nodes[i].overlay.get_subcom(set_vals.community_id) is not None
        assert (
            len(
                set_vals.nodes[i]
                .overlay.get_subcom(set_vals.community_id)
                .get_known_peers()
            )
            > 0
        )


# TODO: Test subscribe multiple communities
