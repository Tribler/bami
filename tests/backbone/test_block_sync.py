from unittest.mock import ANY

from bami.backbone.block import BamiBlock
from bami.backbone.block_sync import BlockSyncMixin
from bami.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
import pytest

from tests.conftest import FakeBlock
from tests.mocking.base import (
    create_and_connect_nodes,
    deliver_messages,
    SetupValues,
    unload_nodes,
)
from tests.mocking.community import MockedCommunity
from tests.mocking.mock_db import MockDBManager


class BlockSyncCommunity(MockedCommunity, BlockSyncMixin):
    def process_block_unordered(self, blk: BamiBlock, peer: Peer) -> None:
        pass

    def received_block_in_order(self, block: BamiBlock) -> None:
        pass


NUM_NODES = 2


@pytest.fixture()
def overlay_class():
    return BlockSyncCommunity


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


def test_init_setup(set_vals):
    assert set_vals.nodes[0].overlay.decode_map[RawBlockBroadcastPayload.msg_id]
    assert set_vals.nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]


@pytest.mark.asyncio
async def test_send_receive_block(monkeypatch, mocker, set_vals):
    blk = FakeBlock(transaction=b"test")
    set_vals.nodes[0].overlay.send_block(blk, [set_vals.nodes[1].overlay.my_peer])
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___, prefix: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)

    spy = mocker.spy(MockDBManager, "has_block")

    await deliver_messages()
    spy.assert_called_with(ANY, blk.hash)


@pytest.mark.asyncio
async def test_send_receive_raw_block(monkeypatch, mocker, set_vals):
    blk = FakeBlock(transaction=b"test")
    set_vals.nodes[0].overlay.send_block(
        blk.pack(), [set_vals.nodes[1].overlay.my_peer]
    )
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___, prefix: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_block")
    await deliver_messages()
    spy.assert_called_with(ANY, blk.hash)


def test_create_block(monkeypatch, mocker, set_vals):
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_block")
    blk = set_vals.nodes[0].overlay.create_signed_block()
    spy.assert_called_with(ANY, blk.hash)


@pytest.mark.asyncio
async def test_send_incorrect_block(monkeypatch, mocker, set_vals):
    blk = FakeBlock(transaction=b"test")
    set_vals.nodes[0].overlay.send_block(
        blk.pack(signature=False), [set_vals.nodes[1].overlay.my_peer]
    )
    spy = mocker.spy(BamiBlock, "unpack")
    spy2 = mocker.spy(BamiBlock, "block_invariants_valid")
    await deliver_messages()
    spy.assert_called_once()
    spy2.assert_called_once()
    assert spy2.spy_return is False
