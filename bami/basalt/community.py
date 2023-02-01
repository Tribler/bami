from asyncio import get_event_loop
import random
import sys
from binascii import unhexlify
from typing import List, Optional, Callable

from bami.basalt.payload import PullPayload, PushPayload, PeerPayload
from bami.basalt.peer import BasaltPeer
from bami.basalt.settings import BasaltSettings
from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.types import Peer


class BasaltCommunity(Community):
    """
    Community for peer sampling with the Basalt algorithm.
    Connected applications should set self.sample_callback that will return a peer on each sampling interval.
    """

    community_id = unhexlify("d37c847a628e1414cffb6a4626b7fa0999fba888")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the Basalt community and required variables.
        """
        self.settings = kwargs.pop("settings", BasaltSettings())
        super(BasaltCommunity, self).__init__(*args, **kwargs)

        self.view: List[Optional[BasaltPeer]] = [None] * self.settings.view_size
        self.hits: List = [0] * self.settings.view_size
        self.seeds: List = [BasaltCommunity.get_seed()] * self.settings.view_size
        self.current_peer_index: int = 0  # r (in the paper)
        self.sample_callback: Optional[Callable[[BasaltPeer], None]] = None

        if self.settings.auto_start_logic:
            self.register_task(
                "check_sufficient_peers",
                self.check_sufficient_peers,
                interval=1.0,
                delay=0,
            )

        self.add_message_handler(PullPayload, self.received_pull)
        self.add_message_handler(PushPayload, self.received_push)

    def check_sufficient_peers(self) -> None:
        """
        Routine after starting the community. When there are enough bootstrap peers, start the view exchange process
        and peer sampling loop.
        """
        known_peers = self.get_peers()
        if len(known_peers) >= self.settings.min_bootstrap_peers:
            self.logger.info("🕸️ <t=%.2f> Basalt bootstrapping finished, found %d peers",
                             get_event_loop().time(), len(self.get_peers()))
            self.cancel_pending_task("check_sufficient_peers")

            # Convert IPv8 peers to Basalt peers
            bootstrap_peers = []
            for peer in known_peers:
                bootstrap_peers.append(BasaltPeer.from_peer(peer))

            self.update_sample(bootstrap_peers)

            tick_interval = (
                self.settings.replacement_count / self.settings.sampling_rate
            )
            self.register_task(
                "basalt_tick", self.on_basalt_tick, interval=tick_interval
            )
            self.register_task(
                "peer_exchange",
                self.peer_update,
                interval=self.settings.time_unit_in_seconds,
            )

    @staticmethod
    def get_seed() -> int:
        """
        Return a random seed.
        """
        return random.randint(1, sys.maxsize)

    def peer_update(self) -> None:
        """
        Called on every time unit.
        Send a pull and push message.
        """
        peer = self.select_peer()
        self.send_pull(peer)
        peer = self.select_peer()
        self.send_push(peer)

    def send_pull(self, peer: Peer) -> None:
        self.ez_send(peer, PullPayload())

    @lazy_wrapper(PullPayload)
    def received_pull(self, peer: Peer, _: PullPayload) -> None:
        self.send_push(peer)

    def send_push(self, peer: Peer):
        """
        Send the (encoded) current view to the target peer.
        """
        peer_payloads = [
            PeerPayload(view_peer.address, view_peer.public_key.key_to_bin())
            for view_peer in self.view
        ]
        push_payload = PushPayload(peer_payloads)
        self.ez_send(peer, push_payload)

    @lazy_wrapper(PushPayload)
    def received_push(self, peer: Peer, payload: PushPayload):
        """
        We received a push message from another peer, so we update our samples.
        """
        peers = []
        self.logger.info("✉️ <t=%.2f> Received push message with %d peer from peer %s",
                         get_event_loop().time(), len(payload.peers), peer)
        for peer_payload in payload.peers:
            peers.append(
                BasaltPeer(peer_payload.public_key, address=peer_payload.address)
            )
        self.update_sample(peers)

    def update_sample(self, peers: List[BasaltPeer]) -> None:
        """
        Update the view with the passed inforamtion.
        """
        for i in range(self.settings.view_size):
            for peer in peers:
                if self.view[i] == peer:
                    self.hits[i] += 1
                elif self.view[i] is None or self.view[i].is_lower_in_rank(
                    peer, self.seeds[i]
                ):
                    self.view[i] = (
                        peer
                        if isinstance(peer, BasaltPeer)
                        else BasaltPeer.from_peer(peer)
                    )
                    self.hits[i] = 1

    def on_basalt_tick(self) -> None:
        """
        Called every tick. Sample some peers and return them to the application.
        """
        for i in range(self.settings.replacement_count):
            self.current_peer_index = (
                self.current_peer_index + 1
            ) % self.settings.view_size
            if self.sample_callback:  # Sample a peer and invoke the callback
                self.sample_callback(self.view[self.current_peer_index])
            self.seeds[self.current_peer_index] = BasaltCommunity.get_seed()
        self.update_sample(self.view)

    def select_peer(self) -> BasaltPeer:
        """
        Return the peer with the lowest number of hits so far.
        """
        min_index = 0
        min_value = sys.maxsize
        for i in range(self.settings.view_size):
            if self.hits[i] < min_value:
                min_value = self.hits[i]
                min_index = i

        self.hits[min_index] += 1
        return self.view[min_index]
