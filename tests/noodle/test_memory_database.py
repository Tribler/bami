import asynctest
from collections import namedtuple
from random import random

from python_project.noodle.memory_database import NoodleMemoryDatabase
from python_project.noodle.block import EMPTY_PK
from tests.test_block import TestBlock


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


class TestMemDB(asynctest.TestCase):
    def setUp(self):
        self.session_id = "".join([chr(i) for i in range(64)])
        self.db = NoodleMemoryDatabase("test", "test")
        self.db2 = NoodleMemoryDatabase("test2", "test")

    def test_add_spend(self, previous=None):
        transaction = {
            "value": random(),
            "from_peer": 1,
            "to_peer": 2,
            "total_spend": 2,
        }
        block = TestBlock(
            transaction=transaction, block_type=b"spend", previous=previous
        )
        from_id = self.db.key_to_id(block.public_key)
        to_id = self.db.key_to_id(block.link_public_key)

        self.db.add_block(block)
        self.assertEqual(
            transaction["total_spend"],
            self.db.work_graph[from_id][to_id]["total_spend"],
        )
        self.assertTrue("spend_num" in self.db.work_graph[from_id][to_id])

        return block

    def test_add_mint(self):
        transaction = {
            "value": random(),
            "from_peer": 0,
            "to_peer": 2,
            "total_spend": 3,
        }
        Linked = namedtuple("Linked", ["public_key", "sequence_number"])
        linked = Linked(EMPTY_PK, 0)
        block = TestBlock(transaction=transaction, block_type=b"claim", linked=linked)
        from_id = self.db.key_to_id(block.public_key)
        to_id = self.db.key_to_id(block.link_public_key)

        self.db.add_block(block)
        self.assertEqual(
            transaction["total_spend"],
            self.db.work_graph[to_id][from_id]["total_spend"],
        )
        self.assertTrue(self.db.work_graph[to_id][from_id]["verified"])
        return block

    def test_add_claim(self, linked=None):
        transaction = {
            "value": random(),
            "from_peer": 0,
            "to_peer": 2,
            "total_spend": 1,
        }
        if linked:
            transaction["total_spend"] = linked.transaction["total_spend"]
        key = linked.link_key if linked else None
        block = TestBlock(
            transaction=transaction, block_type=b"claim", linked=linked, key=key
        )
        if linked:
            self.assertEqual(block.link_public_key, linked.public_key)
            self.assertEqual(block.public_key, linked.link_public_key)
        self.db.add_block(block)
        from_id = self.db.key_to_id(block.public_key)
        to_id = self.db.key_to_id(block.link_public_key)

        if linked:
            self.assertTrue("spend_num" in self.db.work_graph[to_id][from_id])
        self.assertEqual(
            transaction["total_spend"],
            self.db.work_graph[to_id][from_id]["total_spend"],
        )
        self.assertEqual(
            transaction["total_spend"],
            self.db.work_graph[to_id][from_id]["total_spend"],
        )
        if linked:
            self.assertTrue(self.db.get_balance(to_id) >= 0)
        else:
            self.assertTrue(self.db.work_graph[to_id][from_id]["verified"])
        return block

    def test_full_chain(self):
        blk1 = self.test_add_mint()
        val = self.db.get_balance(blk1.public_key)
        blk2 = self.test_add_spend(previous=blk1)
        self.assertEqual(blk1.public_key, blk2.public_key)
        blk3 = self.test_add_claim(linked=blk2)
        from_id = self.db.key_to_id(blk3.public_key)
        to_id = self.db.key_to_id(blk3.link_public_key)

        self.assertTrue(self.db.work_graph[to_id][from_id]["verified"])
        return blk1, blk2, blk3

    def test_invert_insert(self):
        mint, spend, claim = self.test_full_chain()

        self.db2.add_block(claim)
        pid_claim = self.db2.key_to_id(claim.public_key)
        lid_claim = self.db2.key_to_id(claim.link_public_key)
        self.assertEqual(self.db2.get_balance(pid_claim), 2)
        self.assertGreater(self.db2.get_balance(pid_claim, False), 0)
        self.assertLess(self.db2.get_balance(lid_claim), 0)

        self.db2.add_block(spend)
        self.db2.add_block(mint)

        self.assertTrue(
            self.db2.get_last_pairwise_block(spend.public_key, spend.link_public_key)
        )
        blk1, blk2 = self.db2.get_last_pairwise_block(
            spend.public_key, spend.link_public_key
        )
        self.assertEqual(blk1.public_key, blk2.link_public_key)

        self.assertGreater(self.db2.get_balance(pid_claim), 0)
        self.assertGreater(self.db2.get_balance(lid_claim), 0)

        # Test chain dumps
        self.db3 = NoodleMemoryDatabase("q1", "a1")
        status = self.db2.get_peer_status(claim.link_public_key)

        self.db3.dump_peer_status(lid_claim, status)
        self.assertEqual(
            self.db2.get_balance(pid_claim), self.db3.get_balance(pid_claim)
        )
