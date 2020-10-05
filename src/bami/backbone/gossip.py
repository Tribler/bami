from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from asyncio import Queue, sleep
from random import sample, shuffle
from typing import Iterable, Union

from bami.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.backbone.payload import (
    BlocksRequestPayload,
    FrontierPayload,
    FrontierResponsePayload,
    RawBlockPayload,
)
from bami.backbone.sub_community import SubCommunityRoutines
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer


class NextPeerSelectionStrategy(ABC):
    @abstractmethod
    def get_next_gossip_peers(
        self, subcom_id: bytes, chain_id: bytes, my_frontier: Frontier, number: int
    ) -> Iterable[Peer]:
        """
        Get peers for the next gossip round.
        Args:
            subcom_id: identifier for the sub-community
            chain_id: identifier for the chain to gossip
            my_frontier: Current local frontier to share
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
        self,
        subcom_id: bytes,
        chain_id: bytes,
        my_frontier: Frontier,
        number_peers: int,
    ) -> Iterable[Peer]:
        subcom = self.get_subcom(subcom_id)
        peer_set = subcom.get_known_peers() if subcom else []
        f = min(len(peer_set), number_peers)
        return sample(peer_set, f)


class SmartPeerSelectionStrategy(
    NextPeerSelectionStrategy,
    SubCommunityRoutines,
    CommunityRoutines,
    metaclass=ABCMeta,
):
    def get_next_gossip_peers(
        self, subcom_id: bytes, chain_id: bytes, my_frontier: Frontier, number: int
    ) -> Iterable[Peer]:
        subcom = self.get_subcom(subcom_id)
        peer_set = subcom.get_known_peers() if subcom else []

        selected_peers = []
        for p in peer_set:
            known_frontier = self.persistence.get_last_frontier(
                chain_id, p.public_key.key_to_bin()
            )
            if my_frontier > known_frontier:
                selected_peers.append(p)
        shuffle(selected_peers)
        return (
            selected_peers[:number] if len(selected_peers) > number else selected_peers
        )


class GossipRoutines(ABC):
    @property
    @abstractmethod
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        pass

    @abstractmethod
    def frontier_gossip_sync_task(self, subcom_id: bytes) -> None:
        """Start of the gossip state machine"""
        pass

    @abstractmethod
    def incoming_frontier_queue(self, subcom_id: bytes) -> Queue:
        pass


class GossipFrontiersMixin(
    GossipRoutines, MessageStateMachine, CommunityRoutines, metaclass=ABCMeta,
):
    COMMUNITY_CACHE = u"gossip_cache"

    def frontier_gossip_sync_task(self, subcom_id: bytes, prefix: bytes = b"") -> None:
        """Start of the gossip state machine"""
        chain = self.persistence.get_chain(prefix + subcom_id)
        if not chain:
            self.logger.debug(
                "No chain for %s. Skipping the gossip round.", prefix + subcom_id
            )
        if chain:
            frontier = chain.frontier
            # Select next peers for the gossip round
            next_peers = self.gossip_strategy.get_next_gossip_peers(
                subcom_id,
                prefix + subcom_id,
                frontier,
                self.settings.frontier_gossip_fanout,
            )
            for peer in next_peers:
                self.logger.debug(
                    "Sending frontier %s to peer %s. Witness chain: %s",
                    frontier,
                    peer,
                    prefix.startswith(b"w"),
                )
                self.send_packet(
                    peer, FrontierPayload(prefix + subcom_id, frontier.to_bytes())
                )

    async def process_frontier_queue(self, subcom_id: bytes) -> None:
        while True:
            peer, frontier, should_respond = await self.incoming_frontier_queue(
                subcom_id
            ).get()
            self.persistence.store_last_frontier(
                subcom_id, peer.public_key.key_to_bin(), frontier
            )
            frontier_diff = self.persistence.reconcile(
                subcom_id, frontier, peer.public_key.key_to_bin()
            )
            if frontier_diff.is_empty():
                # Move to the next
                await sleep(0.001)
            else:
                # Request blocks and wait for some time
                self.logger.debug(
                    "Sending frontier diff %s to peer %s. Witness chain: %s",
                    frontier_diff,
                    peer,
                    subcom_id.startswith(b"w"),
                )
                self.send_packet(
                    peer, BlocksRequestPayload(subcom_id, frontier_diff.to_bytes())
                )
                await sleep(self.settings.frontier_gossip_collect_time)
            # Send frontier response:
            chain = self.persistence.get_chain(subcom_id)
            if chain and should_respond:
                self.send_packet(
                    peer, FrontierResponsePayload(subcom_id, chain.frontier.to_bytes())
                )

    def process_frontier_payload(
        self,
        peer: Peer,
        payload: Union[FrontierPayload, FrontierResponsePayload],
        should_respond: bool,
    ) -> None:
        frontier = Frontier.from_bytes(payload.frontier)
        chain_id = payload.chain_id
        # Process frontier
        if self.incoming_frontier_queue(chain_id):
            self.incoming_frontier_queue(chain_id).put_nowait(
                (peer, frontier, should_respond)
            )
        else:
            self.logger.error("Received unexpected frontier %s", chain_id)

    @lazy_wrapper(FrontierPayload)
    def received_frontier(self, peer: Peer, payload: FrontierPayload) -> None:
        self.process_frontier_payload(peer, payload, should_respond=True)

    @lazy_wrapper(FrontierResponsePayload)
    def received_frontier_response(
        self, peer: Peer, payload: FrontierResponsePayload
    ) -> None:
        self.process_frontier_payload(peer, payload, should_respond=False)

    @lazy_wrapper(BlocksRequestPayload)
    def received_blocks_request(
        self, peer: Peer, payload: BlocksRequestPayload
    ) -> None:
        f_diff = FrontierDiff.from_bytes(payload.frontier_diff)
        chain_id = payload.subcom_id
        vals_to_request = set()
        self.logger.debug(
            "Received block request %s from peer %s. Witness chain: %s",
            f_diff,
            peer,
            chain_id.startswith(b"w"),
        )
        blocks = self.persistence.get_block_blobs_by_frontier_diff(
            chain_id, f_diff, vals_to_request
        )
        self.logger.debug(
            "Sending %s blocks to peer %s. Witness chain %s",
            len(blocks),
            peer,
            chain_id.startswith(b"w"),
        )
        for block in blocks:
            self.send_packet(peer, RawBlockPayload(block))

    def setup_messages(self) -> None:
        self.add_message_handler(FrontierPayload, self.received_frontier)
        self.add_message_handler(
            FrontierResponsePayload, self.received_frontier_response
        )
        self.add_message_handler(BlocksRequestPayload, self.received_blocks_request)


class SubComGossipMixin(
    GossipFrontiersMixin, SmartPeerSelectionStrategy, metaclass=ABCMeta
):
    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        return self
