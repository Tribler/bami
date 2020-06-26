import pytest
from python_project.backbone.datastore.database import DBManager, ChainTopic
from python_project.backbone.datastore.frontiers import FrontierDiff
from python_project.backbone.datastore.utils import (
    Dot,
    ShortKey,
    encode_raw,
    Ranges,
    Links,
)

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
        monkeypatch.setattr(
            MockChain,
            "add_block",
            lambda _, block_links, seq_num, block_hash: ["dot1", "dot2"]
            if block_hash == self.test_block.hash
            else None,
        )

        def chain_dots_tester(chain_id, chain_dots):
            assert chain_id in (self.test_block.public_key, self.test_block.com_id)
            assert chain_dots == ["dot1", "dot2"]

        self.dbms.add_observer(ChainTopic.ALL, chain_dots_tester)
        self.dbms.add_block(self.block_blob, self.test_block)

    def test_blocks_by_frontier_diff(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore, "get_hash_by_dot", lambda _, dot_bytes: self.test_hash
        )
        monkeypatch.setattr(
            MockBlockStore, "get_block_by_hash", lambda _, blob_hash: self.block_blob
        )
        monkeypatch.setattr(
            MockChain, "get_dots_by_seq_num", lambda _, seq_num: ("dot1", "dot2")
        )

        # init chain
        chain_id = self.chain_id
        self.dbms.chains[chain_id] = MockChain()
        frontier_diff = FrontierDiff(Ranges(((1, 2),)), Links(((1, ShortKey("efef")),)))

        blobs = self.dbms.get_block_blobs_by_frontier_diff(chain_id, frontier_diff)
        assert len(list(blobs)) == 5

    def test_blocks_by_frontier_diff_no_seq_num(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore, "get_hash_by_dot", lambda _, dot_bytes: self.test_hash
        )
        monkeypatch.setattr(
            MockBlockStore, "get_block_by_hash", lambda _, blob_hash: self.block_blob
        )
        monkeypatch.setattr(MockChain, "get_dots_by_seq_num", lambda _, seq_num: list())

        # init chain
        chain_id = self.chain_id
        self.dbms.chains[chain_id] = MockChain()
        frontier_diff = FrontierDiff(Ranges(((1, 2),)), Links(()))

        blobs = self.dbms.get_block_blobs_by_frontier_diff(chain_id, frontier_diff)
        assert len(list(blobs)) == 0

    def test_blocks_by_frontier_diff_no_chain(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore, "get_hash_by_dot", lambda _, dot_bytes: self.test_hash
        )
        monkeypatch.setattr(
            MockBlockStore, "get_block_by_hash", lambda _, blob_hash: self.block_blob
        )
        monkeypatch.setattr(
            MockChain, "get_dots_by_seq_num", lambda _, seq_num: list("dot1")
        )

        # init chain
        chain_id = self.chain_id
        # self.dbms.chains[chain_id] = MockChain()
        frontier_diff = FrontierDiff(Ranges(((1, 1),)), Links(()))

        blobs = self.dbms.get_block_blobs_by_frontier_diff(chain_id, frontier_diff)
        assert len(list(blobs)) == 0
