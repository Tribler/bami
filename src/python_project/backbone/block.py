import logging
import time
from binascii import hexlify
from collections import namedtuple
from hashlib import sha256
from typing import Any

import orjson as json
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.messaging.serialization import default_serializer
from python_project.backbone.datastore.utils import (
    decode_links,
    encode_links,
    shorten,
    Links,
    BytesLinks,
)
from python_project.backbone.filters import BaseLinkFilter, DefaultLinkFilter
from python_project.backbone.payload import BlockPayload

GENESIS_HASH = b"0" * 32  # ID of the first block of the chain.
GENESIS_SEQ = 1
UNKNOWN_SEQ = 0
EMPTY_SIG = b"0" * 64
EMPTY_PK = b"0" * 74
ANY_COUNTERPARTY_PK = EMPTY_PK
SKIP_ATTRIBUTES = {
    "key",
    "serializer",
    "crypto",
    "_logger",
    "_previous",
    "_links",
}


class PlexusBlock(object):
    """
    Container for Plexus block information
    """

    Data = namedtuple(
        "Data",
        [
            "type",
            "transaction",
            "public_key",
            "sequence_number",
            "previous",
            "links",
            "com_id",
            "com_seq_num",
            "timestamp",
            "insert_time",
            "signature",
        ],
    )

    def __init__(self, data=None, serializer=default_serializer):
        """
        Create a new PlexusBlock or load from an existing database entry.

        :param data: Optional data to initialize this block with.
        :type data: TrustChainBlock.Data or list
        :param serializer: An optional custom serializer to use for this block.
        :type serializer: Serializer
        """
        super(PlexusBlock, self).__init__()
        self.serializer = serializer
        if data is None:
            # data
            self.type = b"unknown"
            self.transaction = b""
            # block identity
            self.public_key = EMPTY_PK
            self.sequence_number = (
                GENESIS_SEQ  # sequence number related to the personal chain
            )

            # previous hash in the personal chain
            self.previous = Links({(GENESIS_SEQ - 1, shorten(GENESIS_HASH))})
            self._previous = encode_links(self.previous)

            # Linked blocks => links to the block in other chains
            self.links = Links({(GENESIS_SEQ - 1, shorten(GENESIS_HASH))})
            self._links = encode_links(self.links)

            # Metadata for community identifiers
            self.com_id = EMPTY_PK
            self.com_seq_num = 0

            # Creation timestamp
            self.timestamp = int(time.time() * 1000)
            # Signature for the block
            self.signature = EMPTY_SIG
            # debug stuff
            self.insert_time = None
        else:
            self.transaction = data[1] if isinstance(data[1], bytes) else bytes(data[1])
            self._previous = (
                BytesLinks(data[4]) if isinstance(data[4], bytes) else bytes(data[4])
            )
            self._links = (
                BytesLinks(data[5]) if isinstance(data[5], bytes) else bytes(data[5])
            )

            self.previous = decode_links(self._previous)
            self.links = decode_links(self._links)

            self.type, self.public_key, self.sequence_number = data[0], data[2], data[3]
            self.com_id, self.com_seq_num = data[6], data[7]
            self.signature, self.timestamp, self.insert_time = (
                data[8],
                data[9],
                data[10],
            )

            self.type = (
                self.type
                if isinstance(self.type, bytes)
                else str(self.type).encode("utf-8")
            )
            self.public_key = (
                self.public_key
                if isinstance(self.public_key, bytes)
                else bytes(self.public_key)
            )
            self.signature = (
                self.signature
                if isinstance(self.signature, bytes)
                else bytes(self.signature)
            )

        self.hash = self.calculate_hash()
        self.crypto = default_eccrypto
        self._logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        # This makes debugging and logging easier
        return "Block {0} from ...{1}:{2} links {3} for {4} type {5} cseq {6} cid {7}".format(
            self.short_hash,
            shorten(self.public_key),
            self.sequence_number,
            self.links,
            self.transaction,
            self.type,
            self.com_seq_num,
            self.com_id,
        )

    @property
    def short_hash(self):
        return shorten(self.hash)

    def __hash__(self):
        return self.hash_number

    @property
    def hash_number(self):
        """
        Return the hash of this block as a number (used as crawl ID).
        """
        return int(hexlify(self.hash), 16) % 100000000

    def calculate_hash(self):
        return sha256(self.pack()).digest()

    def __eq__(self, other):
        if not isinstance(other, PlexusBlock):
            return False
        return self.pack() == other.pack()

    @property
    def is_peer_genesis(self):
        return (
            self.sequence_number == GENESIS_SEQ
            and (GENESIS_SEQ - 1, shorten(GENESIS_HASH)) in self.previous
        )

    def pack(self, signature=True):
        """
        Encode this block for transport
        Args:
            signature: False to pack EMPTY_SIG in the signature location, true to pack the signature field
        Returns:
            Block bytes
        """
        args = [
            self.type,
            self.transaction,
            self.public_key,
            self.sequence_number,
            self._previous,
            self._links,
            self.com_id,
            self.com_seq_num,
            self.signature if signature else EMPTY_SIG,
            self.timestamp,
        ]
        return self.serializer.pack_multiple(BlockPayload(*args).to_pack_list())[0]

    @classmethod
    def from_payload(cls, payload, serializer):
        """
        Create a block according to a given payload and serializer.
        This method can be used when receiving a block from the network.
        """
        return cls(
            [
                payload.type,
                payload.transaction,
                payload.public_key,
                payload.sequence_number,
                payload.previous,
                payload.links,
                payload.com_id,
                payload.com_seq_num,
                payload.signature,
                payload.timestamp,
                time.time(),
            ],
            serializer,
        )

    def sign(self, key):
        """
        Signs this block with the given key
        :param key: the key to sign this block with
        """
        self.signature = self.crypto.create_signature(key, self.pack(signature=False))
        self.hash = self.calculate_hash()

    @classmethod
    def create(
        cls,
        block_type: bytes,
        transaction: bytes,
        database: Any,
        public_key: Any,
        link_filter: BaseLinkFilter = None,
        com_id: Any = None,
        links: Links = None,
        fork_seq: int = None,
    ):
        """
        Create PlexusBlock

        Args:
            block_type: type of the block in bytes
            transaction: transaction blob bytes
            database: local database with chains
            public_key: public key of the block creator
            link_filter: Filter object to attack to block that are valid according to the buisness logic
            com_id: id of the community which block is part of [optional]
            links: Explicitly link with these blocks [optional]
            fork_seq: Fork personal chain at this level [optional]

        Returns:
            PlexusBlock

        """

        # Decide to link blocks in the personal chain:
        if fork_seq:
            blks = database.get(public_key, fork_seq)
            # choose any block in blks
            blk = list(blks.values())[0]
            prevs = blk.previous
            seq_num = blk.sequence_number - 1
        else:
            frontier = database.get_lastest_peer_frontier(public_key)
            prevs = None
            seq_num = 0
            if frontier:
                prevs = frontier["v"]
                seq_num = max(frontier["v"])[0]

        if not link_filter:
            link_filter = DefaultLinkFilter()

        ret = cls()
        ret.type = block_type
        ret.transaction = transaction

        # Community related logic
        if com_id:
            ret.com_id = com_id
            # There is community specified => will base block on the latest known information + filters
            if links:
                linked = links
                link_seq_num = max(links)[0]
            else:
                frontier = database.get_latest_community_frontier(com_id)
                linked = link_filter.filter(frontier["v"] if frontier else set())
                link_seq_num = max(linked)[0] if linked else 0

            ret.links = linked
            ret.com_seq_num = link_seq_num + 1
            ret.com_id = com_id

        if prevs:
            ret.previous = prevs
            ret.sequence_number = seq_num + 1

        ret._links = encode_links(ret.links)
        ret._previous = encode_links(ret.previous)

        ret.public_key = public_key
        ret.signature = EMPTY_SIG
        ret.hash = ret.calculate_hash()
        return ret

    def __iter__(self):
        """
        This override allows one to take the dict(<block>) of a block.
        :return: generator to iterate over all properties of this block
        """
        for key, value in self.__dict__.items():
            if key in SKIP_ATTRIBUTES:
                continue
            if key == "transaction":
                yield key, self.transaction
            elif key == "links":
                yield key, decode_links(self._links)
            elif key == "previous":
                yield key, decode_links(self._previous)
            elif isinstance(value, bytes) and key != "insert_time" and key != "type":
                yield key, hexlify(value).decode("utf-8")
            else:
                yield key, value.decode("utf-8") if isinstance(value, bytes) else value
