from abc import ABCMeta, abstractmethod
from typing import Union, Iterable, Optional

from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from bami.backbone.block import BamiBlock
from bami.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.backbone.utils import Links, AUDIT_TYPE, encode_raw
from bami.backbone.exceptions import InvalidBlockException
from bami.backbone.payload import (
    RawBlockBroadcastPayload,
    BlockBroadcastPayload,
    RawBlockPayload,
    BlockPayload,
)


class BlockSyncMixin(MessageStateMachine, CommunityRoutines, metaclass=ABCMeta):
    def setup_messages(self) -> None:
        self.add_message_handler(RawBlockPayload, self.received_raw_block)
        self.add_message_handler(BlockPayload, self.received_block)
        self.add_message_handler(
            RawBlockBroadcastPayload, self.received_raw_block_broadcast
        )
        self.add_message_handler(BlockBroadcastPayload, self.received_block_broadcast)

    def send_block(
        self, block: Union[BamiBlock, bytes], peers: Iterable[Peer], ttl: int = 1
    ) -> None:
        """
        Send a block to the set of peers. If ttl is higher than 1: will gossip the message further.
        Args:
            block: block to send
            peers: set of peers
            ttl: Time to live for the message. If > 1 - this is a multi-hop message
        """
        if ttl > 1:
            # This is a block for gossip
            packet = (
                RawBlockBroadcastPayload(block, ttl)
                if type(block) is bytes
                else BlockBroadcastPayload(*block.block_args(), ttl)
            )
        else:
            packet = (
                RawBlockPayload(block)
                if type(block) is bytes
                else block.to_block_payload()
            )
        for p in peers:
            self.send_packet(p, packet)

    @lazy_wrapper(RawBlockPayload)
    def received_raw_block(self, peer: Peer, payload: RawBlockPayload) -> None:
        block = BamiBlock.unpack(payload.block_bytes, self.serializer)
        self.logger.debug(
            "Received block from pull gossip %s from peer %s", block.com_dot, peer
        )
        self.validate_persist_block(block, peer)

    @lazy_wrapper(BlockPayload)
    def received_block(self, peer: Peer, payload: BlockPayload):
        block = BamiBlock.from_payload(payload, self.serializer)
        self.logger.debug(
            "Received block from push gossip %s from peer %s", block.com_dot, peer
        )
        self.validate_persist_block(block, peer)

    @lazy_wrapper(RawBlockBroadcastPayload)
    def received_raw_block_broadcast(
        self, peer: Peer, payload: RawBlockBroadcastPayload
    ) -> None:
        block = BamiBlock.unpack(payload.block_bytes, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_broadcast_block(block, payload.ttl)

    @lazy_wrapper(BlockBroadcastPayload)
    def received_block_broadcast(self, peer: Peer, payload: BlockBroadcastPayload):
        block = BamiBlock.from_payload(payload, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_broadcast_block(block, payload.ttl)

    def process_broadcast_block(self, block: BamiBlock, ttl: int):
        """Process broadcast block and relay further"""
        if block.hash not in self.relayed_broadcasts and ttl > 1:
            # self.send_block(block, ttl=ttl - 1)
            pass

    @abstractmethod
    def process_block_unordered(self, blk: BamiBlock, peer: Peer) -> None:
        """
        Process a received half block immediately when received. Does not guarantee order on the block.
        """
        pass

    @abstractmethod
    def received_block_in_order(self, block: BamiBlock) -> None:
        """
        Process a block that we have received.

        Args:
            block: The received block.

        """
        pass

    def validate_persist_block(self, block: BamiBlock, peer: Peer = None) -> bool:
        """
        Validate a block and if it's valid, persist it.
        Raises:
            InvalidBlockException - if block is not valid
        """
        block = (
            BamiBlock.unpack(block, self.serializer) if type(block) is bytes else block
        )
        block_blob = block if type(block) is bytes else block.pack()

        if not block.block_invariants_valid():
            # React on invalid block
            raise InvalidBlockException("Block invalid", str(block), peer)
        else:
            if not self.persistence.has_block(block.hash):
                self.process_block_unordered(block, peer)
                chain_id = block.com_id
                prefix = block.com_prefix
                if (
                    self.persistence.get_chain(prefix + chain_id)
                    and self.persistence.get_chain(prefix + chain_id).versions.get(
                        block.com_seq_num
                    )
                    and block.short_hash
                    in self.persistence.get_chain(prefix + chain_id).versions[
                        block.com_seq_num
                    ]
                ):
                    raise Exception(
                        "Inconsisistency between block store and chain store",
                        self.persistence.get_chain(prefix + chain_id).versions,
                        block.com_dot,
                    )
                self.persistence.add_block(block_blob, block)

    def create_signed_block(
        self,
        block_type: bytes = b"unknown",
        transaction: Optional[bytes] = None,
        prefix: bytes = b"",
        com_id: bytes = None,
        links: Links = None,
        personal_links: Links = None,
        use_consistent_links: bool = True,
    ) -> BamiBlock:
        """
        This function will create, sign, persist block with given parameters.
        Args:
            block_type: bytes of the block
            transaction: bytes blob of the transaction, or None to indicate an empty transaction payload
            prefix: prefix for the community id. For example b'w' - for witnessing transactions
            com_id: sub-community id if applicable
            links: explicitly link to certain links in the sub-community. Warning - may lead to forks!
            personal_links: explicitly link to certain blocks in the own chain. Warning - may lead to forks!
            use_consistent_links ():
        Returns:
            signed block
        """
        if not transaction:
            transaction = encode_raw(b"")

        block = BamiBlock.create(
            block_type,
            transaction,
            self.persistence,
            self.my_pub_key_bin,
            com_id=com_id,
            com_links=links,
            pers_links=personal_links,
            com_prefix=prefix,
            use_consistent_links=use_consistent_links,
        )
        block.sign(self.my_peer_key)
        self.validate_persist_block(block, self.my_peer)
        return block
