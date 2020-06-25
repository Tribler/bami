import itertools
from typing import Any

import pytest
from python_project.backbone.datastore.database import BaseDB
from python_project.backbone.datastore.state_store import State
from python_project.backbone.datastore.utils import Dot, encode_raw, GENESIS_DOT, Links

from tests.conftest import TestBlock
from tests.mocking.mock_db import MockDBManager, MockChain


class KVState(State):
    def __init__(self, back_db: BaseDB) -> None:
        super().__init__(back_db)
        self.state_dict = {GENESIS_DOT: 0}

    def apply_tx(self, chain_id: bytes, prev_links: Links, dot: Dot, tx: Any) -> bool:
        id_val = tx.get("val")

        for prev_dot in prev_links:
            if prev_dot in self.state_dict:
                # replace in state
                self.state_dict.pop(prev_dot)
        self.state_dict[dot] = id_val
        return True


class BaseTestConvergentStates:
    @pytest.fixture
    def std_com_vals(self, monkeypatch, create_batches):
        txs = []
        self.B = 4
        self.N = 10
        for i in range(self.B):
            txs_batch = list()
            for j in range(self.N):
                txs_batch.append(encode_raw({"val": (i + 1) * (j + 1)}))
            txs.append(txs_batch)
        self.blocks = create_batches(self.B, self.N, txs=txs)

        dot_tx_map = {}
        prev_links = {}

        self.dots = list()
        self.chain_id = None

        for i in range(self.B):
            batch_dots = list()
            for j in range(self.N):
                pers_dot = Dot(
                    (self.blocks[i][j].sequence_number, self.blocks[i][j].short_hash)
                )
                com_dot = Dot(
                    (self.blocks[i][j].com_seq_num, self.blocks[i][j].short_hash)
                )

                com_id = self.blocks[i][j].com_id

                batch_dots.append(com_dot)
                self.chain_id = com_id
                dot_tx_map[(com_id, com_dot)] = self.blocks[i][j].transaction
                prev_links[com_dot] = self.blocks[i][j].links
            self.dots.append(batch_dots)

        # noinspection PyTypeChecker
        self.last_links = Links(
            tuple(
                (
                    self.blocks[i][self.N - 1].com_seq_num,
                    self.blocks[i][self.N - 1].short_hash,
                )
                for i in range(self.B)
            )
        )
        self.last_block = TestBlock(
            transaction=encode_raw({"val": 100}),
            links=self.last_links,
            com_id=self.chain_id,
        )
        self.last_dot = Dot((self.last_block.com_seq_num, self.last_block.short_hash))
        dot_tx_map[(self.chain_id, self.last_dot)] = self.last_block.transaction
        prev_links[self.last_dot] = self.last_block.links

        monkeypatch.setattr(
            MockDBManager,
            "get_tx_blob_by_dot",
            lambda _, chain_id, dot: dot_tx_map.get((chain_id, dot)),
        )
        monkeypatch.setattr(
            MockDBManager, "get_chain", lambda _, chain_id: MockChain(),
        )
        monkeypatch.setattr(
            MockChain, "get_prev_links", lambda _, dot: prev_links.get(dot),
        )


class TestKVState(BaseTestConvergentStates):
    @pytest.fixture(autouse=True)
    def setUp(self) -> None:
        self.back_db = MockDBManager()
        self.state_obj = KVState(self.back_db)

    def test_receive_empty_dot(self, std_com_vals):
        self.state_obj.receive_chain_dots(None, None)
        assert self.state_obj.state_dict == {GENESIS_DOT: 0}

    def test_receive_one_dot(self, std_com_vals):
        dots = self.dots[0][:1]

        self.state_obj.receive_chain_dots(self.chain_id, dots)
        assert len(self.state_obj.state_dict) == 1
        vals = list(self.state_obj.state_dict.items())
        assert vals[0][1] == 1 and vals[0][0][0] == 1

    def test_receive_chain_dots(self, std_com_vals):
        dots = self.dots[0]

        self.state_obj.receive_chain_dots(self.chain_id, dots)
        assert len(self.state_obj.state_dict) == 1
        vals = list(self.state_obj.state_dict.items())
        assert vals[0][1] == self.N and vals[0][0][0] == self.N

    @pytest.mark.parametrize(
        "dot_fork", [k for k in itertools.permutations(range(4), 4)]
    )
    def test_receive_fork_chains(self, std_com_vals, dot_fork):
        for i in range(4):
            self.state_obj.receive_chain_dots(self.chain_id, self.dots[i])
        assert len(self.state_obj.state_dict) == 4
        vals = list(self.state_obj.state_dict.items())
        for i in range(4):
            assert vals[i][1] == (i + 1) * 10

        self.state_obj.receive_chain_dots(self.chain_id, [self.last_dot])
        assert len(self.state_obj.state_dict) == 1
        vals = list(self.state_obj.state_dict.items())
        assert vals[0][1] == 100
        assert vals[0][0][0] == 11
