from abc import ABC, abstractmethod
from typing import Iterable

from ipv8.peer import Peer
from python_project.backbone.community_routines import CommonRoutines
from python_project.backbone.payload import SubscriptionsPayload


class SubCommunityGossipStrategy:
    pass


class SubCommunityMixin(CommonRoutines):
    @property
    @abstractmethod
    def my_subcoms(self) -> Iterable[bytes]:
        pass

    @abstractmethod
    def encode_subcom(self, subcom: Iterable[bytes]) -> bytes:
        pass

    @abstractmethod
    def add_subcom(self, sub_com: bytes) -> None:
        pass

    def is_subscribed(self, community_id: bytes) -> bool:
        return community_id in self.my_subcoms

    @abstractmethod
    def get_subcom_notify_peers(self) -> Iterable[Peer]:
        pass

    @abstractmethod
    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        pass

    def subscribe_to_subcoms(self, subcoms: Iterable[bytes]) -> None:
        """
        Subscribe to the sub communities with given ids

        If bootstrap_master is not specified will use RandomWalks to discover other peers for the same community.
        Peer will be connect to maximum  `settings.max_peers_subtrust` peers.
        Args:
            subcoms: Iterable object with sub_community ids
        """
        for c_id in subcoms:
            if c_id not in self.my_subcoms:
                self.add_subcom(c_id)
                # Join the sub-community
                self.join_subcommunity_gossip(c_id)

        # Find other peers in the community
        for p in self.get_subcom_notify_peers():
            # Send them new subscribe collection
            self.send_peer_subs(p.address, self.my_subcoms)

    def subscribe_to_subcom(self, subcom_id: bytes) -> None:
        """
        Subscribe to the SubCommunity with the public key master peer.
        Community is identified with a community_id.

        Args:
            subcom_id: bytes identifier of the community
            personal: this is community is on personal chain
        """
        if subcom_id not in self.my_subcoms:
            self.add_subcom(subcom_id)

            # Join the protocol audits/ updates
            self.join_subcommunity_gossip(subcom_id)

            # Notify other peers that you are part of the new community
            for peer in self.get_subcom_notify_peers():
                # Send them new subscribe collection
                self.send_peer_subs(peer, self.my_subcoms)

    def send_peer_subs(self, peer: Peer, peer_subs: Iterable[bytes]) -> None:
        """
        Send to selected peer update message
        """
        subs_packet = self.encode_subcom(peer_subs)
        self.send_packet(peer, SubscriptionsPayload(self.my_pub_key, subs_packet))

    @abstractmethod
    def received_peer_subs(self, peer: Peer, payload: SubscriptionsPayload) -> None:
        pass
