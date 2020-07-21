import pytest
from bami.backbone.datastore.block_store import LMDBLockStore
from bami.backbone.datastore.chain_store import ChainFactory
from bami.backbone.datastore.database import ChainTopic, DBManager
from bami.backbone.datastore.frontiers import FrontierDiff
from bami.backbone.utils import (
    Dot,
    encode_raw,
    Ranges,
    ShortKey,
    wrap_iterate,
)

from tests.conftest import FakeBlock
from tests.mocking.mock_db import MockBlockStore, MockChain, MockChainFactory


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

        self.test_block = FakeBlock()
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

        def chain_dots_tester(chain_id, dots):
            assert chain_id in (self.test_block.public_key, self.test_block.com_id)
            assert dots == ["dot1", "dot2"]

        self.dbms.add_observer(ChainTopic.ALL, chain_dots_tester)
        self.dbms.add_block(self.block_blob, self.test_block)

    def test_blocks_by_frontier_diff(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore, "get_hash_by_dot", lambda _, dot_bytes: bytes(dot_bytes)
        )
        monkeypatch.setattr(
            MockBlockStore, "get_block_by_hash", lambda _, blob_hash: bytes(blob_hash)
        )
        monkeypatch.setattr(
            MockChain, "get_dots_by_seq_num", lambda _, seq_num: ("dot1", "dot2")
        )

        # init chain
        chain_id = self.chain_id
        self.dbms.chains[chain_id] = MockChain()
        frontier_diff = FrontierDiff(Ranges(((1, 2),)), {(1, ShortKey("efef")): {}})
        vals_to_request = set()

        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            chain_id, frontier_diff, vals_to_request
        )
        assert len(vals_to_request) == 0
        assert len(blobs) == 3

    def test_blocks_frontier_with_extra_request(self, monkeypatch, std_vals):
        monkeypatch.setattr(
            MockBlockStore, "get_hash_by_dot", lambda _, dot_bytes: self.test_hash
        )
        monkeypatch.setattr(
            MockBlockStore, "get_block_by_hash", lambda _, blob_hash: self.block_blob
        )
        monkeypatch.setattr(
            MockChain, "get_dots_by_seq_num", lambda _, seq_num: ("dot1", "dot2")
        )

        local_vers = {2: {"ef1"}, 7: {"ef1"}}

        monkeypatch.setattr(
            MockChain,
            "get_all_short_hash_by_seq_num",
            lambda _, seq_num: local_vers.get(seq_num),
        )
        monkeypatch.setattr(
            MockChain,
            "get_next_links",
            lambda _, dot: ((dot[0] + 1, ShortKey("efef")),),
        )

        # init chain
        chain_id = self.chain_id
        self.dbms.chains[chain_id] = MockChain()
        frontier_diff = FrontierDiff(
            (), {(10, ShortKey("efef")): {2: ("ef1",), 7: ("ef2",)}}
        )

        set_to_request = set()
        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            chain_id, frontier_diff, set_to_request
        )
        assert len(set_to_request) == 1
        assert len(blobs) == 1

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
        frontier_diff = FrontierDiff(Ranges(((1, 2),)), {})

        set_to_request = set()
        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            chain_id, frontier_diff, set_to_request
        )
        assert len(set_to_request) == 0
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
        frontier_diff = FrontierDiff(Ranges(((1, 1),)), {})

        set_to_request = set()
        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            chain_id, frontier_diff, set_to_request
        )
        assert len(set_to_request) == 0
        assert len(list(blobs)) == 0


class TestIntegrationDBManager:
    @pytest.fixture(autouse=True)
    def setUp(self, tmpdir) -> None:
        tmp_val = tmpdir
        self.block_store = LMDBLockStore(str(tmp_val))
        self.chain_factory = ChainFactory()
        self.dbms = DBManager(self.chain_factory, self.block_store)
        yield
        self.dbms.close()

    @pytest.fixture(autouse=True)
    def setUp2(self, tmpdir) -> None:
        tmp_val = tmpdir
        self.block_store2 = LMDBLockStore(str(tmp_val))
        self.chain_factory2 = ChainFactory()
        self.dbms2 = DBManager(self.chain_factory2, self.block_store2)
        yield
        try:
            self.dbms2.close()
            tmp_val.remove()
        except FileNotFoundError:
            pass

    def test_get_tx_blob(self):
        self.test_block = FakeBlock()
        packed_block = self.test_block.pack()
        self.dbms.add_block(packed_block, self.test_block)
        self.tx_blob = self.test_block.transaction

        assert (
            self.dbms.get_tx_blob_by_dot(
                self.test_block.com_id, self.test_block.com_dot
            )
            == self.tx_blob
        )
        assert (
            self.dbms.get_block_blob_by_dot(
                self.test_block.com_id, self.test_block.com_dot
            )
            == packed_block
        )

    def test_add_notify_block_one_chain(self, create_batches, insert_function):
        self.val_dots = []

        def chain_dots_tester(chain_id, dots):
            print(dots)
            for dot in dots:
                assert (len(self.val_dots) == 0 and dot[0] == 1) or dot[
                    0
                ] == self.val_dots[-1][0] + 1
                self.val_dots.append(dot)

        blks = create_batches(num_batches=1, num_blocks=100)
        com_id = blks[0][0].com_id
        self.dbms.add_observer(com_id, chain_dots_tester)

        wrap_iterate(insert_function(self.dbms, blks[0]))
        assert len(self.val_dots) == 100

    def test_add_notify_block_with_conflicts(self, create_batches, insert_function):
        self.val_dots = []

        def chain_dots_tester(chain_id, dots):
            for dot in dots:
                self.val_dots.append(dot)

        blks = create_batches(num_batches=2, num_blocks=100)
        com_id = blks[0][0].com_id
        self.dbms.add_observer(com_id, chain_dots_tester)

        wrap_iterate(insert_function(self.dbms, blks[0][:20]))
        wrap_iterate(insert_function(self.dbms, blks[1][:40]))
        wrap_iterate(insert_function(self.dbms, blks[0][20:60]))
        wrap_iterate(insert_function(self.dbms, blks[1][40:]))
        wrap_iterate(insert_function(self.dbms, blks[0][60:]))

        assert len(self.val_dots) == 200

    def test_blocks_by_frontier_diff(self, create_batches, insert_function):
        # init chain
        blks = create_batches(num_batches=2, num_blocks=100)
        com_id = blks[0][0].com_id

        wrap_iterate(insert_function(self.dbms, blks[0][:50]))
        wrap_iterate(insert_function(self.dbms2, blks[1][:50]))

        front = self.dbms.get_chain(com_id).frontier
        front_diff = self.dbms2.get_chain(com_id).reconcile(front)
        print(front_diff)
        vals_request = set()

        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            com_id, front_diff, vals_request
        )
        assert len(blobs) == 41

    def reconcile_round(self, com_id):
        front = self.dbms.get_chain(com_id).frontier
        front_diff = self.dbms2.get_chain(com_id).reconcile(front)
        print("Frontier diff", front_diff)
        vals_request = set()
        blobs = self.dbms.get_block_blobs_by_frontier_diff(
            com_id, front_diff, vals_request
        )
        return blobs

    def test_blocks_by_fdiff_with_holes(self, create_batches, insert_function):
        # init chain
        blks = create_batches(num_batches=2, num_blocks=100)
        com_id = blks[0][0].com_id
        self.val_dots = []

        def chain_dots_tester(chain_id, dots):
            for dot in dots:
                self.val_dots.append(dot)

        self.dbms2.add_observer(com_id, chain_dots_tester)

        wrap_iterate(insert_function(self.dbms, blks[0][:50]))
        wrap_iterate(insert_function(self.dbms2, blks[1][:20]))
        wrap_iterate(insert_function(self.dbms2, blks[1][40:60]))

        assert len(self.val_dots) == 20
        blobs = self.reconcile_round(com_id)
        assert len(blobs) == 41

        for b in blobs:
            self.dbms2.add_block(b, FakeBlock.unpack(b, blks[0][0].serializer))

        assert len(self.val_dots) == 20
        print("VAl dots", self.val_dots)
        blobs2 = self.reconcile_round(com_id)
        assert len(blobs2) == 8
        for b in blobs2:
            self.dbms2.add_block(b, FakeBlock.unpack(b, blks[0][0].serializer))

        assert len(self.val_dots) == 20
        blobs2 = self.reconcile_round(com_id)
        assert len(blobs2) == 1
        for b in blobs2:
            self.dbms2.add_block(b, FakeBlock.unpack(b, blks[0][0].serializer))
        assert len(self.val_dots) == 70
        print(self.val_dots)
