from __future__ import annotations

from bami.basalt.util import hash

from ipv8.peer import Peer


class BasaltPeer(Peer):
    """
    An IPv8 peer with some additional utility methods.
    """

    @staticmethod
    def from_peer(peer: Peer) -> BasaltPeer:
        return BasaltPeer(peer.key, address=peer.address)

    def get_ip_prefix_8(self) -> str:
        return ".".join(self.address[0].split(".")[:1])

    def get_ip_prefix_16(self) -> str:
        return ".".join(self.address[0].split(".")[:2])

    def get_ip_prefix_24(self) -> str:
        return ".".join(self.address[0].split(".")[:3])

    def is_lower_in_rank(self, other_peer: BasaltPeer, seed: int) -> bool:
        """
        Return true iff other_peer is lower in rank than this peer.

        This check is hierarchical and is based on the IPv4 prefixes
        (first check /8, then /16, then /24 and finally the whole IP).
        """
        if hash(other_peer.get_ip_prefix_8(), seed) < hash(
            self.get_ip_prefix_8(), seed
        ):
            return True
        elif hash(other_peer.get_ip_prefix_8(), seed) > hash(
            self.get_ip_prefix_8(), seed
        ):
            return False

        if hash(other_peer.get_ip_prefix_16(), seed) < hash(
            self.get_ip_prefix_16(), seed
        ):
            return True
        elif hash(other_peer.get_ip_prefix_16(), seed) > hash(
            self.get_ip_prefix_16(), seed
        ):
            return False

        if hash(other_peer.get_ip_prefix_24(), seed) < hash(
            self.get_ip_prefix_24(), seed
        ):
            return True
        elif hash(other_peer.get_ip_prefix_24(), seed) > hash(
            self.get_ip_prefix_24(), seed
        ):
            return False

        return hash(other_peer.address[0], seed) < hash(self.address[0], seed)
