import pytest
from ipv8.keyvault.crypto import default_eccrypto
from python_project.backbone.block import PlexusBlock, EMPTY_PK, UNKNOWN_SEQ
from python_project.backbone.datastore.utils import (
    shorten,
    encode_raw,
    Links,
    GENESIS_DOT,
    ShortKey,
)
from python_project.backbone.payload import BlockPayload
from python_project.noodle.block import (
    EMPTY_SIG,
    GENESIS_SEQ,
    ValidationResult,
)

from tests.conftest import TestBlock
from tests.mocking.mock_db import MockDBManager, MockChain


class TestChainBlock:
    """
    This class contains tests for a TrustChain block.
    """

    def test_sign(self):
        """
        Test signing a block and whether the signature is valid
        """
        crypto = default_eccrypto
        block = TestBlock()
        assert crypto.is_valid_signature(
            block.key, block.pack(signature=False), block.signature
        )

    def test_short_hash(self):
        block = TestBlock()
        assert shorten(block.hash) == block.short_hash

    def test_create_genesis(self):
        """
        Test creating a genesis block
        """
        key = default_eccrypto.generate_key(u"curve25519")
        db = MockDBManager()
        block = PlexusBlock.create(
            b"test", encode_raw({"id": 42}), db, key.pub().key_to_bin()
        )

        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == encode_raw({"id": 42})
        assert block.com_id == EMPTY_PK
        assert block.com_seq_num == UNKNOWN_SEQ

    def test_create_next_pers(self, monkeypatch):
        """
        Test creating a block that points towards a previous block
        """
        db = MockDBManager()
        prev = TestBlock()

        monkeypatch.setattr(
            MockDBManager, "get_chain", lambda _, chain_id: MockChain(),
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((prev.pers_dot,)),
        )

        block = PlexusBlock.create(b"test", encode_raw({"id": 42}), db, prev.public_key)

        assert block.previous == Links((prev.pers_dot,))
        assert block.sequence_number == prev.sequence_number + 1
        assert block.public_key == prev.public_key
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == encode_raw({"id": 42})
        assert block.com_id == EMPTY_PK
        assert block.com_seq_num == UNKNOWN_SEQ

    def test_create_link_to_pers(self, monkeypatch):
        """
        Test creating a linked half block
        """
        key = default_eccrypto.generate_key(u"curve25519")
        db = MockDBManager()
        link = TestBlock()

        monkeypatch.setattr(
            MockDBManager,
            "get_chain",
            lambda _, chain_id: MockChain() if chain_id == link.public_key else None,
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((link.pers_dot,)),
        )
        block = PlexusBlock.create(
            b"test",
            encode_raw({"id": 42}),
            db,
            key.pub().key_to_bin(),
            com_id=link.public_key,
        )

        # include the personal community

        # Attach to the
        assert block.links == Links((link.pers_dot,))
        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.com_seq_num == link.sequence_number + 1
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == encode_raw({"id": 42})
        assert block.com_id == link.public_key

    def test_create_link_to_com_chain(self, monkeypatch):
        """
        Test creating a linked half that points back towards a previous block
        """
        key = default_eccrypto.generate_key(u"curve25519")
        com_key = default_eccrypto.generate_key(u"curve25519").pub().key_to_bin()
        db = MockDBManager()
        com_link = Links(((1, ShortKey("30303030")),))
        link = TestBlock(com_id=com_key, links=com_link)

        monkeypatch.setattr(
            MockDBManager,
            "get_chain",
            lambda _, chain_id: MockChain() if chain_id == com_key else None,
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((link.com_dot,)),
        )
        block = PlexusBlock.create(
            b"test", encode_raw({"id": 42}), db, key.pub().key_to_bin(), com_id=com_key
        )

        # include the personal community

        # Attach to the
        assert block.links == Links((link.com_dot,))
        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.com_seq_num == link.com_seq_num + 1
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == encode_raw({"id": 42})
        assert block.com_id == com_key

    def test_invariant_negative_timestamp(self):
        """
        Test if negative sequence number blocks are not valid.
        """
        block = TestBlock()
        block.timestamp = -1.0
        assert not block.block_invariants_valid()

    def test_invariant_invalid_key(self):
        """
        Test if illegal key blocks are not valid.
        """
        result = ValidationResult()
        block = TestBlock()
        block.public_key = b"definitelynotakey"
        assert not block.block_invariants_valid()

    def test_invariant_invalid_seq_num(self):
        """
        Test if illegal key blocks are not valid.
        """
        result = ValidationResult()
        block = TestBlock()
        block.sequence_number = -1
        assert not block.block_invariants_valid()

    def test_invariant_invalid_com_seq_num(self):
        """
        Test if illegal key blocks are not valid.
        """
        result = ValidationResult()
        block = TestBlock()
        block.com_seq_num = -1
        assert not block.block_invariants_valid()

    def test_invalid_sign(self):
        key = default_eccrypto.generate_key(u"curve25519")

        blk = TestBlock()
        blk.sign(key)

        assert not blk.block_invariants_valid()

    def test_block_valid(self):
        blk = TestBlock()
        assert blk.block_invariants_valid()

    def test_block_payload(self):
        blk = TestBlock()
        blk_bytes = blk.pack()
        unpacked = blk.serializer.ez_unpack_serializables([BlockPayload], blk_bytes)
        blk2 = PlexusBlock.from_payload(unpacked[0])
        assert blk2 == blk

    def test_pack_unpack(self):
        blk = TestBlock()
        blk_bytes = blk.pack()
        blk2 = PlexusBlock.unpack(blk_bytes, blk.serializer)
        assert blk == blk2

    @pytest.mark.skip
    def test_iter(self):
        """
        Check that the iterator of a Block has all of the required keys without duplicates.
        """
        block = TestBlock()
        block_keys = []
        for field in iter(block):
            block_keys.append(field[0])
        expected_keys = {
            "public_key",
            "transaction",
            "hash",
            "timestamp",
            "link_sequence_number",
            "insert_time",
            "previous_hash",
            "sequence_number",
            "signature",
            "link_public_key",
            "type",
            "transaction_validation_result",
        }
        # Check if we have the required keys
        self.assertSetEqual(set(block_keys) - {"link_key"}, expected_keys)
        # Check for duplicates
        self.assertEqual(len(block_keys) - 1, len(expected_keys))
        self.assertEqual(dict(block)["transaction"]["id"], 42)

    def test_hash_function(self):
        """
        Check if the hash() function returns the Block hash.
        """
        block = TestBlock()

        assert block.__hash__(), block.hash_number
