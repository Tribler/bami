from unittest.mock import ANY

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
from ipv8.test.mocking.endpoint import internet
import pytest
from python_project.backbone.block import PlexusBlock
from python_project.backbone.block_sync import BlockSyncMixin
from python_project.backbone.exceptions import InvalidBlockException
from python_project.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)

from tests.conftest import FakeBlock
from tests.mocking.base import TestBase
from tests.mocking.community import MockedCommunity
from tests.mocking.mock_db import MockDBManager


class BlockSyncCommunity(MockedCommunity, BlockSyncMixin):
    def process_block_unordered(self, blk: PlexusBlock, peer: Peer) -> None:
        pass


class TestBlockSync(TestBase):
    NUM_NODES = 2

    @pytest.yield_fixture(autouse=True)
    def main_fix(self):
        self.nodes = []
        internet.clear()
        self._tempdirs = []

        super().setUp()
        self.initialize(BlockSyncCommunity, self.NUM_NODES)

        # TODO: Add additional setup
        # Make sure every node has a community to listen to
        self.community_key = default_eccrypto.generate_key(u"curve25519").pub()
        self.community_id = self.community_key.key_to_bin()
        # for node in self.nodes:
        #    node.overlay.subscribe_to_subcom(self.community_id)
        yield

    @pytest.mark.asyncio
    async def test_init_setup(self):
        assert chr(RawBlockBroadcastPayload.msg_id) in self.nodes[0].overlay.decode_map
        assert chr(BlockBroadcastPayload.msg_id) in self.nodes[0].overlay.decode_map
        await self.tearDown()

    @pytest.mark.asyncio
    async def test_send_receive_block(self, monkeypatch, mocker):
        blk = FakeBlock(transaction=b"test")
        self.nodes[0].overlay.send_block(blk, [self.nodes[1].overlay.my_peer])
        monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
        monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)

        spy = mocker.spy(MockDBManager, "has_block")

        await self.deliver_messages()
        spy.assert_called_with(ANY, blk.hash)
        await self.tearDown()

    @pytest.mark.asyncio
    async def test_send_receive_raw_block(self, monkeypatch, mocker):
        blk = FakeBlock(transaction=b"test")
        self.nodes[0].overlay.send_block(blk.pack(), [self.nodes[1].overlay.my_peer])
        monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
        monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
        spy = mocker.spy(MockDBManager, "has_block")
        await self.deliver_messages()
        spy.assert_called_with(ANY, blk.hash)
        await self.tearDown()

    @pytest.mark.asyncio
    async def test_create_block(self, monkeypatch, mocker):
        monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
        monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
        spy = mocker.spy(MockDBManager, "has_block")
        blk = self.nodes[0].overlay.create_signed_block()
        spy.assert_called_with(ANY, blk.hash)
        await self.tearDown()

    @pytest.mark.asyncio
    @pytest.mark.xfail(raises=InvalidBlockException)
    async def test_send_incorrect_block(self, monkeypatch, mocker):
        blk = FakeBlock(transaction=b"test")
        self.nodes[0].overlay.send_block(
            blk.pack(signature=False), [self.nodes[1].overlay.my_peer]
        )
        spy = mocker.spy(PlexusBlock, "unpack")
        spy2 = mocker.spy(PlexusBlock, "block_invariants_valid")
        await self.deliver_messages()
        spy.assert_called_once()
        spy2.assert_called_once()
        assert spy2.spy_return is False
        await self.tearDown()
