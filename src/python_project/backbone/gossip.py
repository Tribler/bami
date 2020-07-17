from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from random import sample
from typing import Iterable, List, Optional, Tuple

from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from ipv8.requestcache import NumberCache

from python_project.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from python_project.backbone.datastore.frontiers import Frontier, FrontierDiff
from python_project.backbone.datastore.utils import expand_ranges, hex_to_int
from python_project.backbone.payload import (
    BlocksRequestPayload,
    FrontierPayload,
    RawBlockPayload,
)
from python_project.backbone.sub_community import SubCommunityRoutines


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


class GossipFrontierSyncCache(NumberCache):
    """
    This cache works as queue that tracks outstanding sync requests with other peers in a community
    """

    def __init__(self, community: GossipFrontiersMixin, chain_id: bytes) -> None:
        cache_num = hex_to_int(chain_id)
        self.community = community

        self.chain_id = chain_id
        self.working_front = dict()

        NumberCache.__init__(
            self, community.request_cache, community.COMMUNITY_CACHE, cache_num
        )

    @property
    def timeout_delay(self):
        return self.community.settings.gossip_collect_time

    def receive_frontier(self, peer: Peer, frontier: Frontier) -> None:
        # TODO: add verification for the frontier. Hiding transactions?
        self.working_front[peer.public_key.key_to_bin()] = (peer, frontier)

    def process_working_front(self) -> List[Optional[Tuple[Peer, FrontierDiff]]]:
        candidate = None
        cand_max = 0

        for peer_key, peer_front in self.working_front.items():
            frontier_diff = self.community.persistence.reconcile(
                self.chain_id, peer_front[1], peer_key
            )

            num = len(expand_ranges(frontier_diff.missing)) + len(
                frontier_diff.conflicts
            )

            if not frontier_diff.is_empty() and num > cand_max:
                candidate = (peer_front[0], frontier_diff)
                cand_max = num
        return [candidate]

    def on_timeout(self):
        # TODO convert this to a queue
        async def add_later():
            try:
                self.community.request_cache.add(
                    GossipFrontierSyncCache(self.community, self.chain_id)
                )
            except RuntimeError:
                # TODO add logger reaction here
                pass

        # Process received frontiers
        candidates = self.process_working_front()
        # Send requests to candidates
        for cand in candidates:
            # Send request to candidate peer
            if cand:
                self.community.send_packet(
                    cand[0], BlocksRequestPayload(self.chain_id, cand[1].to_bytes())
                )


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
                self.send_packet(peer, FrontierPayload(subcom_id, frontier.to_bytes()))

    @lazy_wrapper(FrontierPayload)
    def received_frontier(self, peer: Peer, payload: FrontierPayload) -> None:
        frontier = Frontier.from_bytes(payload.frontier)
        chain_id = payload.chain_id
        # Process frontier
        cache = self.request_cache.get(
            GossipFrontiersMixin.COMMUNITY_CACHE, hex_to_int(chain_id)
        )
        if cache:
            cache.receive_frontier(peer, frontier)
        else:
            # Create new cache
            diff = self.persistence.reconcile(
                chain_id, frontier, peer.public_key.key_to_bin()
            )
            if not diff.is_empty():
                # Request blocks from the peer
                self.send_packet(peer, BlocksRequestPayload(chain_id, diff.to_bytes()))
                self.request_cache.add(GossipFrontierSyncCache(self, chain_id))

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
