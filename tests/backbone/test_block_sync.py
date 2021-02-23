from unittest.mock import ANY

from bami.backbone.block import BamiBlock
from bami.backbone.block_sync import BlockSyncMixin
from bami.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
from ipv8.peer import Peer
import pytest

from tests.conftest import FakeBlock
from tests.mocking.base import deliver_messages
from tests.mocking.community import MockedCommunity
from tests.mocking.mock_db import MockDBManager


class BlockSyncCommunity(MockedCommunity, BlockSyncMixin):
    def process_block_unordered(self, blk: BamiBlock, peer: Peer) -> None:
        pass

    def received_block_in_order(self, block: BamiBlock) -> None:
        pass


@pytest.fixture()
def overlay_class():
    return BlockSyncCommunity


@pytest.fixture()
def init_nodes():
    return False


@pytest.fixture()
def num_nodes():
    return 2


def test_init_setup(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.decode_map[RawBlockBroadcastPayload.msg_id]
    assert set_vals_by_key.nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]


@pytest.mark.asyncio
async def test_send_receive_block(monkeypatch, mocker, set_vals_by_key):
    blk = FakeBlock(transaction=b"test")
    set_vals_by_key.nodes[0].overlay.send_block(
        blk, [set_vals_by_key.nodes[1].overlay.my_peer]
    )
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___, prefix: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)

    spy = mocker.spy(MockDBManager, "has_block")

    await deliver_messages()
    spy.assert_called_with(ANY, blk.hash)


@pytest.mark.asyncio
async def test_send_receive_raw_block(monkeypatch, mocker, set_vals_by_key):
    blk = FakeBlock(transaction=b"test")
    set_vals_by_key.nodes[0].overlay.send_block(
        blk.pack(), [set_vals_by_key.nodes[1].overlay.my_peer]
    )
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___, prefix: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_block")
    await deliver_messages()
    spy.assert_called_with(ANY, blk.hash)


def test_create_block(monkeypatch, mocker, set_vals_by_key):
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_block")
    blk = set_vals_by_key.nodes[0].overlay.create_signed_block()
    spy.assert_called_with(ANY, blk.hash)


@pytest.mark.asyncio
async def test_send_incorrect_block(monkeypatch, mocker, set_vals_by_key):
    blk = FakeBlock(transaction=b"test")
    set_vals_by_key.nodes[0].overlay.send_block(
        blk.pack(signature=False), [set_vals_by_key.nodes[1].overlay.my_peer]
    )
    spy = mocker.spy(BamiBlock, "unpack")
    spy2 = mocker.spy(BamiBlock, "block_invariants_valid")
    await deliver_messages()
    spy.assert_called_once()
    spy2.assert_called_once()
    assert spy2.spy_return is False
