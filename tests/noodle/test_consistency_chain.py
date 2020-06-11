import unittest
from binascii import hexlify, unhexlify
from hashlib import sha256
from typing import Any, List

import orjson as json
import pytest
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.private.libnaclkey import LibNaCLSK
from python_project.backbone.block import (
    EMPTY_SIG,
    GENESIS_HASH,
    PlexusBlock,
    GENESIS_SEQ,
)
from python_project.backbone.datastore.consistency import ChainState, Chain
from python_project.backbone.datastore.memory_database import PlexusMemoryDatabase
from python_project.backbone.datastore.utils import (
    shorten,
    encode_links,
    Links,
    ShortKey,
    decode_raw,
)


class TestBlock(PlexusBlock):
    """
    Test Block that simulates a block used in TrustChain.
    Also used in other test files for TrustChain.
    """

    def __init__(
            self,
            transaction: bytes = b'{"id": 42}',
            previous: Links = None,
            key: LibNaCLSK = None,
            links: Links = None,
            com_id: Any = None,
            block_type: bytes = b"test",
    ):
        crypto = default_eccrypto
        if not links:
            links = tuple({(0, shorten(GENESIS_HASH))})
            com_seq_num = 1
        else:
            com_seq_num = max(links)[0] + 1

        if not previous:
            previous = tuple({(0, shorten(GENESIS_HASH))})
        pers_seq_num = max(previous)[0] + 1

        if not com_id:
            com_id = crypto.generate_key(u"curve25519").pub().key_to_bin()

        if key:
            self.key = key
        else:
            self.key = crypto.generate_key(u"curve25519")

        PlexusBlock.__init__(
            self,
            (
                block_type,
                transaction,
                self.key.pub().key_to_bin(),
                pers_seq_num,
                encode_links(previous),
                encode_links(links),
                com_id,
                com_seq_num,
                EMPTY_SIG,
                0,
                0,
            ),
        )
        self.sign(self.key)


class MockDatabase(PlexusMemoryDatabase):
    """
    This mocked database is only used during the tests.
    """

    def __init__(self):
        PlexusMemoryDatabase.__init__(self, "", "mock")


def test_chain_terminal_conflict():
    key = default_eccrypto.generate_key(u"curve25519")
    com_id = key.pub().key_to_bin()
    block = TestBlock(com_id=com_id)
    links = Links(((block.com_seq_num, block.short_hash),))
    print(links)
    block2 = TestBlock(com_id=com_id, links=links)
    chain = Chain()
    chain.add_block(block)
    chain.add_block(block2)

    print(chain.chain)
    print(chain.forward_pointers)

    init_link = (0, ShortKey("30303030"))

    print(chain.terminal)
    chain._recalc_terminal(init_link[0], init_link[1])
    print(chain.terminal)

    # Add conflicting block at level 1
    c_block = TestBlock(com_id=com_id)
    chain.add_block(c_block)
    # Conflicting block at level 2
    c_block2 = TestBlock(
        com_id=com_id, links=Links(((c_block.com_seq_num, c_block.short_hash),))
    )
    chain.add_block(c_block2)

    new_link = (c_block.com_seq_num, c_block.short_hash)
    chain._recalc_terminal(new_link[0], new_link[1])
    print(chain.terminal)


def create_block_batch(com_id, num_blocks=100):
    blocks = []
    last_block_point = Links(((0, ShortKey("30303030")),))
    for k in range(num_blocks):
        blk = TestBlock(com_id=com_id, links=last_block_point)
        blocks.append(blk)
        last_block_point = Links(((blk.com_seq_num, blk.short_hash),))
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100):
        key = default_eccrypto.generate_key(u"curve25519")
        com_id = key.pub().key_to_bin()
        return [create_block_batch(com_id, num_blocks) for _ in range(num_batches)]

    return _create_batches


def insert_batch_seq(chain: Chain, batch: List[PlexusBlock]) -> None:
    for blk in batch:
        chain.add_block(blk)
        last_block_link = (blk.com_seq_num, blk.short_hash)
        chain._recalc_terminal(*last_block_link)


def insert_batch_reversed(chain: Chain, batch: List[PlexusBlock]) -> None:
    for blk in reversed(batch):
        chain.add_block(blk)
        last_block_link = (blk.com_seq_num, blk.short_hash)
        chain._recalc_terminal(*last_block_link)


def insert_batch_random(chain: Chain, batch: List[PlexusBlock]) -> None:
    from random import shuffle

    shuffle(batch)
    for blk in batch:
        chain.add_block(blk)
        last_block_link = (blk.com_seq_num, blk.short_hash)
        chain._recalc_terminal(*last_block_link)


def test_seq_insert(create_batches):
    chain = Chain()
    batches = create_batches(num_batches=1, num_blocks=10)
    insert_batch_seq(chain, batches[0])
    # Recalculate terminal after batch insert
    print(chain.terminal)
    assert len(chain.terminal) == 1


def test_reversed_insert(create_batches):
    chain = Chain()
    batches = create_batches(num_batches=1, num_blocks=1000)
    insert_batch_reversed(chain, batches[0])
    assert len(chain.terminal) == 1


def test_random_insert(create_batches):
    chain = Chain()
    batches = create_batches(num_batches=1, num_blocks=1000)
    insert_batch_random(chain, batches[0])
    assert len(chain.terminal) == 1


batch_insert_functions = [insert_batch_seq, insert_batch_random, insert_batch_reversed]


@pytest.mark.parametrize("insert1", batch_insert_functions)
@pytest.mark.parametrize("insert2", batch_insert_functions)
def test_two_conflict_seq_insert(create_batches, insert1, insert2):
    chain = Chain()
    batches = create_batches(num_batches=2, num_blocks=200)

    # Insert first batch sequentially
    last_blk = batches[0][-1]
    last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
    insert1(chain, batches[0])
    assert len(chain.terminal) == 1
    assert last_blk_link in chain.terminal

    # Insert second batch sequentially
    last_blk = batches[1][-1]
    last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
    insert2(chain, batches[1])
    assert len(chain.terminal) == 2
    assert last_blk_link in chain.terminal


@pytest.mark.parametrize("num_batches", [2 ** i for i in range(4, 12, 2)])
@pytest.mark.parametrize("insert_func", batch_insert_functions)
def test_insert_many_conflicts(create_batches, num_batches, insert_func):
    chain = Chain()
    batches = create_batches(num_batches=num_batches, num_blocks=5)
    # Insert first batch sequentially
    i = 1
    for batch in batches:
        last_blk = batches[i - 1][-1]
        insert_func(chain, batch)
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        assert len(chain.terminal) == i
        assert last_blk_link in chain.terminal
        i += 1


class TestPlexusBlocks(unittest.TestCase):
    """
    This class contains tests for a TrustChain block.
    """

    def setUp(self) -> None:
        self.key = default_eccrypto.generate_key(u"curve25519")
        self.db = MockDatabase()
        self.block = PlexusBlock.create(
            b"test", b'{"id": 42}', self.db, self.key.pub().key_to_bin()
        )

    def test_sign(self):
        """
        Test signing a block and whether the signature is valid
        """
        crypto = default_eccrypto
        block = TestBlock()
        self.assertTrue(
            crypto.is_valid_signature(
                block.key, block.pack(signature=False), block.signature
            )
        )

    def test_short_hash(self):
        self.assertEqual(shorten(self.block.hash), self.block.short_hash)

    def test_hash(self):
        self.assertEqual(hash(self.block), self.block.hash_number)

    def test_equal(self):
        class NotPlexusBlock(object):
            pass

        not_block = NotPlexusBlock()
        self.assertFalse(self.block == not_block)
        block1 = TestBlock(key=self.key, com_id=self.key.pub().key_to_bin())
        block2 = TestBlock(key=self.key, com_id=self.key.pub().key_to_bin())
        self.assertEqual(block1, block2)

    def test_create_genesis(self):
        """
        Test creating a genesis block
        """
        block = self.block
        self.assertIn((0, shorten(GENESIS_HASH)), block.previous)
        self.assertTrue(block.is_peer_genesis)
        self.assertEqual(block.public_key, self.key.pub().key_to_bin())
        self.assertEqual(GENESIS_SEQ, block.sequence_number)
        self.assertEqual(block.signature, EMPTY_SIG)
        self.assertEqual(block.type, b"test")

    def test_sign_state(self):
        key = default_eccrypto.generate_key(u"curve25519")
        state = {"val": 100}
        state_blob = json.dumps(state)
        state_hash = sha256(state_blob).digest()
        signature = default_eccrypto.create_signature(key, state_hash)

        # create an audit proof
        my_id = hexlify(key.pub().key_to_bin()).decode()
        sig = hexlify(signature).decode()
        st_h = hexlify(state_hash).decode()

        audit = (my_id, sig, st_h)
        val = json.dumps(audit)
        unval = json.loads(val)

        pub_key = default_eccrypto.key_from_public_bin(unhexlify(unval[0]))
        sign = unhexlify(unval[1])
        hash_val = unhexlify(unval[2])

        self.assertTrue(default_eccrypto.is_valid_signature(pub_key, hash_val, sign))

    def test_create_next(self):
        """
        Test creating a block that points towards a previous block in a personal chain
        """
        db = MockDatabase()
        key = default_eccrypto.generate_key(u"curve25519")
        prev = TestBlock(key=key)
        db.add_block(prev)
        block = PlexusBlock.create(b"test", b"{'id': 42}", db, prev.public_key)

        self.assertEqual({(1, shorten(prev.hash))}, block.previous)
        self.assertEqual(block.sequence_number, 2)
        self.assertEqual(block.public_key, prev.public_key)

    def test_create_community_next(self):
        """
        Test creating a linked half block
        """
        com_key = default_eccrypto.generate_key(u"curve25519").pub().key_to_bin()
        # Generate community id
        gen = TestBlock(com_id=com_key)
        db = MockDatabase()
        db.add_block(gen)
        key = default_eccrypto.generate_key(u"curve25519")
        block = PlexusBlock.create(
            b"test", b'{"id": 42}', db, key.pub().key_to_bin(), com_id=com_key
        )

        self.assertEqual({(1, shorten(gen.hash))}, block.links)
        self.assertEqual(2, block.com_seq_num)
        self.assertEqual(com_key, block.com_id)


class TestPlexusConsistency(unittest.TestCase):
    def test_personal_chain_no_previous(self):
        """
        Scenario: a peer creates a block (seq = 2), linking to a missing block (seq = 1).
        The frontier is now block 2.
        """
        db = MockDatabase()
        block = TestBlock(previous=Links({(1, ShortKey("1234"))}))
        db.add_block(block)
        front = db.get_frontier(block.public_key)
        # It's a frontier in a personal chain
        self.assertTrue(front["p"])
        # Frontier should contain seq_num=2
        self.assertEqual(True, [2 in tuples for tuples in front["v"]][0])
        # Frontier should indicate holes
        self.assertEqual([(1, 1)], front["h"])

    def test_community_chain_no_previous(self):
        """
        Scenario: a peer creates a block in some community, linking to a missing previous block.
        """
        com_key = default_eccrypto.generate_key(u"curve25519").pub().key_to_bin()
        block = TestBlock(com_id=com_key, links=Links({(1, ShortKey("1234"))}))
        db = MockDatabase()
        db.add_block(block)
        front = db.get_frontier(com_key)
        self.assertFalse(front["p"])
        # Frontier should contain seq_num=2
        self.assertEqual(True, [2 in tuples for tuples in front["v"]][0])
        # Frontier should indicate holes
        self.assertEqual([(1, 1)], front["h"])

    def test_personal_chain_inconsistency(self):
        """
        Scenario: a peer makes a block with seq = 1 (A), another block with seq = 2 that points to A,
        and another block with seq = 2 that points to an unexisting block.
        """
        key = default_eccrypto.generate_key(u"curve25519")
        db = MockDatabase()
        block1 = TestBlock(key=key)
        db.add_block(block1)
        block2 = TestBlock(key=key, previous=Links({(1, shorten(block1.hash))}))
        db.add_block(block2)
        block3 = TestBlock(key=key, previous=Links({(1, ShortKey("123445"))}))
        db.add_block(block3)
        front = db.get_frontier(block1.public_key)
        # Frontier should contain the two blocks with seq 2
        self.assertEqual(2, len(front["v"]))
        # Frontier should have no missing holes
        self.assertFalse(front["h"])

    def test_community_chain_inconsistency(self):
        """
        Scenario: a peer makes a community block (A) with com seq = 1, then someone else links to (A),
        and then another peer links to an unexisting community block.
        """
        com_key = default_eccrypto.generate_key(u"curve25519").pub().key_to_bin()
        db = MockDatabase()
        block1 = TestBlock(com_id=com_key)
        db.add_block(block1)
        block2 = TestBlock(com_id=com_key, links=Links({(1, shorten(block1.hash))}))
        db.add_block(block2)
        block3 = TestBlock(com_id=com_key, links=Links({(1, ShortKey("12345"))}))
        db.add_block(block3)
        front = db.get_frontier(com_key)
        # Frontier should contain the two blocks with seq 2
        self.assertEqual(2, len(front["v"]))
        # Frontier should have no missing holes
        self.assertFalse(front["h"])

    def test_iter(self):
        """
        Check that the iterator of a Block has all of the required keys without duplicates.
        """
        block = TestBlock()
        block_keys = []
        for field in iter(block):
            block_keys.append(field[0])
        expected_keys = set(PlexusBlock.Data._fields)
        # Check if we have the required keys
        self.assertSetEqual(expected_keys | {"hash"}, set(block_keys))
        # Check for duplicates
        self.assertEqual(len(block_keys) - 1, len(expected_keys))

        self.assertEqual(decode_raw(block.transaction).get("id"), 42)

    def test_reconcilation(self):
        db1 = MockDatabase()
        block = TestBlock()
        block2 = TestBlock(
            com_id=block.com_id, links=Links({(block.com_seq_num, block.short_hash)})
        )
        db1.add_block(block)
        db1.add_block(block2)

        db2 = MockDatabase()
        block2 = TestBlock(
            transaction=b'{"id": 43}',
            com_id=block.com_id,
            links=Links({(block.com_seq_num, block.short_hash)}),
        )
        db2.add_block(block)
        db2.add_block(block2)

        to_request, to_send = db1.reconcile(
            block.com_id, db2.get_frontier(block.com_id)
        )
        self.assertEqual(list(to_request["c"])[0][0], 2)
        to_request, to_send = db2.reconcile(
            block.com_id, db1.get_frontier(block.com_id)
        )
        self.assertEqual(list(to_request["c"])[0][0], 2)

    def test_reconcilation_with_state(self):
        db1 = MockDatabase()
        db2 = MockDatabase()
        block = TestBlock()
        com_id = block.com_id
        db1.add_chain_state(com_id, MockChainState("sum"))
        db2.add_chain_state(com_id, MockChainState("sum"))

        # db2.add_block(block)
        db1.add_block(block)

        block2 = TestBlock(
            transaction=b'{"id": 40}',
            com_id=block.com_id,
            links=Links({(block.com_seq_num, block.short_hash)}),
        )

        db1.add_block(block2)

        block2 = TestBlock(
            transaction=b'{"id": 43}',
            com_id=block.com_id,
            links=Links({(block.com_seq_num, block.short_hash)}),
        )

        db1.add_block(block2)

        # db2.add_block(block2)

        # to_request, to_send = db1.reconcile(block.com_id, db2.get_frontier(block.com_id))
        # self.assertEqual(list(to_request['c'])[0][0], 2)
        # to_request, to_send = db2.reconcile(block.com_id, db1.get_frontier(block.com_id))
        # self.assertEqual(list(to_request['c'])[0][0], 2)

        # TODO: add state asserts
        # print(db1.get_state(com_id, 0))
        # print(db1.get_state(com_id, 1))
        # print(db1.get_state(com_id, 2))


class MockChainState(ChainState):
    def __init__(self, name):
        super().__init__(name)

    def init_state(self):
        """
        Initialize state when there no blocks
        @return: Fresh new state
        """
        return {"total": 0, "vals": [0, 0], "front": list(), "stakes": dict()}

    def apply_block(self, prev_state, block):
        """
        Apply block(with delta) to the prev_state
        @param prev_state:
        @param block:
        @return: Return new_state
        """
        # 1. Calculate delta between state and transaction
        # get from  front last value
        id_val = decode_raw(block.transaction).get("id")
        delta = id_val - prev_state["vals"][0]
        sh_hash = shorten(block.hash)
        peer = shorten(block.public_key)
        total = prev_state["total"] + abs(delta)
        new_stakes = dict()
        new_stakes.update(prev_state["stakes"])
        if peer not in prev_state["stakes"]:
            new_stakes[peer] = abs(delta)
        else:
            new_stakes[peer] += abs(delta)

        return {
            "total": total,
            "front": [sh_hash],
            "vals": [id_val, delta, peer],
            "stakes": new_stakes,
        }

    def merge(self, old_state, new_state):
        """
        Merge two potentially conflicting states
        @param old_state:
        @param new_state:
        @return: Fresh new state of merged states
        """
        if not old_state:
            # There are no conflicts
            return new_state

        # Check if there are actually conflicting by verifying the fronts
        merged_state = dict()
        if not set(new_state["front"]).issubset(set(old_state["front"])):
            # merge fronts
            merged_state["front"] = sorted(
                list(set(old_state["front"]) | set(new_state["front"]))
            )
            merged_state["total"] = old_state["total"] + abs(new_state["vals"][1])
            merged_state["vals"] = [
                old_state["vals"][0] + new_state["vals"][1],
                old_state["vals"][1] + new_state["vals"][1],
            ]
            p = new_state["vals"][2]
            delta = new_state["vals"][1]
            merged_state["stakes"] = dict()
            merged_state["stakes"].update(old_state["stakes"])
            if p not in merged_state["stakes"]:
                merged_state["stakes"][p] = abs(delta)
            else:
                merged_state["stakes"][p] = merged_state["stakes"][p] + abs(delta)
            merged_state["stakes"] = sorted(merged_state["stakes"].items())

            return merged_state
        else:
            return old_state
