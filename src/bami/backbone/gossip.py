from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from asyncio import Queue, sleep
from random import sample
from typing import Iterable

from bami.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.backbone.payload import (
    BlocksRequestPayload,
    FrontierPayload,
    RawBlockPayload,
)
from bami.backbone.sub_community import SubCommunityRoutines
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer


class NextPeerSelectionStrategy(ABC):
    @abstractmethod
    def get_next_gossip_peers(self, subcom_id: bytes, number: int) -> Iterable[Peer]:
        """
        Get peers for the next gossip round.
        Args:
            subcom_id: identifier for the sub-community
            number: Number of peers to request

        Returns:
            Iterable with peers for the next gossip round
        """
        pass


class RandomPeerSelectionStrategy(
    NextPeerSelectionStrategy,
    SubCommunityRoutines,
    CommunityRoutines,
    metaclass=ABCMeta,
):
    def get_next_gossip_peers(
        self, subcom_id: bytes, number_peers: int
    ) -> Iterable[Peer]:
        subcom = self.get_subcom(subcom_id)
        peer_set = subcom.get_known_peers() if subcom else []
        f = min(len(peer_set), number_peers)
        return sample(peer_set, f)


class GossipRoutines(ABC):
    @property
    @abstractmethod
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        pass

    @abstractmethod
    def gossip_sync_task(self, subcom_id: bytes) -> None:
        """Start of the gossip state machine"""
        pass

    @abstractmethod
    def incoming_frontier_queue(self, subcom_id: bytes) -> Queue:
        pass


class GossipFrontiersMixin(
    GossipRoutines, MessageStateMachine, CommunityRoutines, metaclass=ABCMeta,
):
    COMMUNITY_CACHE = u"gossip_cache"

    def gossip_sync_task(self, subcom_id: bytes) -> None:
        """Start of the gossip state machine"""
        chain = self.persistence.get_chain(subcom_id)
        if chain:
            frontier = chain.frontier
            # Select next peers for the gossip round
            next_peers = self.gossip_strategy.get_next_gossip_peers(
                subcom_id, self.settings.gossip_fanout
            )
            for peer in next_peers:
                self.logger.debug("Sending frontier %s to peer %s", frontier, peer)
                self.send_packet(peer, FrontierPayload(subcom_id, frontier.to_bytes()))

    async def process_frontier_queue(self, subcom_id: bytes):
        while True:
            _delta = self.settings.gossip_collect_time
            peer, frontier = await self.incoming_frontier_queue(subcom_id).get()
            frontier_diff = self.persistence.reconcile(
                subcom_id, frontier, peer.public_key.key_to_bin()
            )
            if frontier_diff.is_empty():
                # Move to the next
                await sleep(0.001)
            else:
                # Request blocks and wait for some time
                self.logger.debug("Sending frontier diff %s to peer %s", frontier_diff, peer)
                self.send_packet(
                    peer, BlocksRequestPayload(subcom_id, frontier_diff.to_bytes())
                )
                await sleep(self.settings.gossip_collect_time)

    @lazy_wrapper(FrontierPayload)
    def received_frontier(self, peer: Peer, payload: FrontierPayload) -> None:
        frontier = Frontier.from_bytes(payload.frontier)
        chain_id = payload.chain_id
        # Process frontier
        self.incoming_frontier_queue(chain_id).put_nowait((peer, frontier))

    @lazy_wrapper(BlocksRequestPayload)
    def received_blocks_request(
        self, peer: Peer, payload: BlocksRequestPayload
    ) -> None:
        f_diff = FrontierDiff.from_bytes(payload.frontier_diff)
        chain_id = payload.subcom_id
        vals_to_request = set()
        blocks = self.persistence.get_block_blobs_by_frontier_diff(
            chain_id, f_diff, vals_to_request
        )
        self.logger.debug("Sending %s blocks to peer %s", len(blocks), peer)
        for block in blocks:
            self.send_packet(peer, RawBlockPayload(block), sig=False)

    def setup_messages(self) -> None:
        self.add_message_handler(FrontierPayload, self.received_frontier)
        self.add_message_handler(BlocksRequestPayload, self.received_blocks_request)


class SubComGossipMixin(
    GossipFrontiersMixin, RandomPeerSelectionStrategy, metaclass=ABCMeta
):
    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        return self
