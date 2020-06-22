import pytest
from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.database import DBManager, ChainTopic
from python_project.backbone.datastore.utils import Dot, ShortKey, encode_raw

from tests.conftest import TestBlock
from tests.mocking.mock_db import MockBlockStore, MockChainFactory, MockChain


class TestDBManager:
    @pytest.fixture(autouse=True)
    def setUp(self) -> None:
        self.block_store = MockBlockStore()
        self.chain_factory = MockChainFactory()
        self.dbms = DBManager(self.chain_factory, self.block_store)

    @pytest.fixture
    def std_vals(self):
        self.chain_id = b"chain_id"
        self.block_dot = Dot((3, ShortKey("808080")))
        self.block_dot_encoded = encode_raw(self.block_dot)
        self.dot_id = self.chain_id + self.block_dot_encoded

        self.test_hash = b"test_hash"
        self.tx_blob = b"tx_blob"
        self.block_blob = b"block_blob"

        self.test_block = TestBlock()
        self.pers = self.test_block.public_key
        self.com_id = self.test_block.com_id

    def test_get_tx_blob(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore,
            "get_hash_by_dot",
            lambda _, dot_bytes: self.test_hash if dot_bytes == self.dot_id else None,
        )
        monkeypatch.setattr(
            MockBlockStore,
            "get_tx_by_hash",
            lambda _, tx_hash: self.tx_blob if tx_hash == self.test_hash else None,
        )

        assert (
            self.dbms.get_tx_blob_by_dot(self.chain_id, self.block_dot) == self.tx_blob
        )

    def test_get_block_blob(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore,
            "get_hash_by_dot",
            lambda _, dot_bytes: self.test_hash if dot_bytes == self.dot_id else None,
        )
        monkeypatch.setattr(
            MockBlockStore,
            "get_block_by_hash",
            lambda _, blob_hash: self.block_blob
            if blob_hash == self.test_hash
            else None,
        )

        assert (
            self.dbms.get_block_blob_by_dot(self.chain_id, self.block_dot)
            == self.block_blob
        )

    def test_add_notify_block(self, monkeypatch, std_vals):
        class MockSerializer:
            def serialize(ser_self, block_blob: bytes) -> PlexusBlock:
                return self.test_block

        monkeypatch.setattr(
            MockChain,
            "add_block",
            lambda _, parsed_block: ["dot1", "dot2"]
            if parsed_block == self.test_block
            else None,
        )

        def chain_dots_tester(chain_id, chain_dots):
            assert chain_id in (self.test_block.public_key, self.test_block.com_id)
            assert chain_dots == ["dot1", "dot2"]

        self.dbms.add_observer(ChainTopic.ALL, chain_dots_tester)
        self.dbms.add_block(self.block_blob, MockSerializer())
