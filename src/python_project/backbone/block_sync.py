from abc import ABCMeta, abstractmethod
from typing import Union, Iterable

from ipv8.lazy_community import lazy_wrapper_unsigned
from ipv8.peer import Peer
from python_project.backbone.block import PlexusBlock
from python_project.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from python_project.backbone.datastore.utils import Links
from python_project.backbone.exceptions import InvalidBlockException
from python_project.backbone.payload import (
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
        self, block: Union[PlexusBlock, bytes], peers: Iterable[Peer], ttl: int = 1
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
            self.send_packet(p, packet, sig=False)

    @lazy_wrapper_unsigned(RawBlockPayload)
    def received_raw_block(self, peer: Peer, payload: RawBlockPayload) -> None:
        block = PlexusBlock.unpack(payload.block_bytes, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_block_unordered(block, peer)

    @lazy_wrapper_unsigned(BlockPayload)
    def received_block(self, peer: Peer, payload: BlockPayload):
        print("Received block from ", peer)
        block = PlexusBlock.from_payload(payload, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_block_unordered(block, peer)

    @lazy_wrapper_unsigned(RawBlockBroadcastPayload)
    def received_raw_block_broadcast(
        self, peer: Peer, payload: RawBlockBroadcastPayload
    ) -> None:
        block = PlexusBlock.unpack(payload.block_bytes, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_block_unordered(block, peer)
        self.process_broadcast_block(block, payload.ttl)

    @lazy_wrapper_unsigned(BlockBroadcastPayload)
    def received_block_broadcast(self, peer: Peer, payload: BlockBroadcastPayload):
        block = PlexusBlock.from_payload(payload, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_block_unordered(block, peer)
        self.process_broadcast_block(block, payload.ttl)

    def process_broadcast_block(self, block: PlexusBlock, ttl: int):
        """Process broadcast block and relay further"""
        if block.hash not in self.relayed_broadcasts and ttl > 1:
            # self.send_block(block, ttl=ttl - 1)
            pass

    @abstractmethod
    def process_block_unordered(self, blk: PlexusBlock, peer: Peer) -> None:
        """
        Process a received half block immediately when received. Does not guarantee order on the block.
        """
        pass

    def validate_persist_block(self, block: PlexusBlock, peer: Peer = None) -> bool:
        """
        Validate a block and if it's valid, persist it.
        Raises:
            InvalidBlockException - if block is not valid
        """
        block = (
            PlexusBlock.unpack(block, self.serializer)
            if type(block) is bytes
            else block
        )
        block_blob = block if type(block) is bytes else block.pack()

        if not block.block_invariants_valid():
            # React on invalid block
            raise InvalidBlockException("Block invalid", str(block), peer)
        else:
            print("Processing block")
            if not self.persistence.has_block(block.hash):
                if (
                    self.persistence.get_chain(block.com_id)
                    and self.persistence.get_chain(block.com_id).versions.get(
                        block.com_seq_num
                    )
                    and block.short_hash
                    in self.persistence.get_chain(block.com_id).versions[
                        block.com_seq_num
                    ]
                ):
                    raise Exception(
                        "Inconsisistency between block store and chain store",
                        self.persistence.get_chain(block.com_id).versions,
                        block.com_dot,
                    )
                print("Putting block to db")
                self.persistence.add_block(block_blob, block)

    def create_signed_block(
        self,
        block_type: bytes = b"unknown",
        transaction: bytes = b"",
        com_id: bytes = None,
        links: Links = None,
        personal_links: Links = None,
    ) -> PlexusBlock:
        """
        This function will create, sign, persist block with given parameters.
        Args:
            block_type: bytes of the block
            transaction: bytes blob of the transaction
            com_id: sub-community id if applicable
            links: explicitly link to certain links in the sub-community. Warning - may lead to forks!
            personal_links: explicitly link to certain blocks in the own chain. Warning - may lead to forks!
        Returns:
            signed block
        """
        block = PlexusBlock.create(
            block_type,
            transaction,
            self.persistence,
            self.my_pub_key_bin,
            com_id,
            links,
            personal_links,
        )
        block.sign(self.my_peer_key)
        self.validate_persist_block(block, self.my_peer)
        return block
