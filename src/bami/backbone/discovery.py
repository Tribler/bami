from abc import ABC, abstractmethod
from typing import Dict, Any, Iterable

from ipv8.peer import Peer
from ipv8.peerdiscovery.discovery import RandomWalk, EdgeWalk


class SubCommunityDiscoveryStrategy(ABC):

    def __init__(self, ipv8):
        self.ipv8 = ipv8

    @abstractmethod
    def discover(
        self,
        subcom: "BaseSubCommunity",
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        """
        Discovery routine for the sub-community.
        Args:
            subcom: SubCommunity object or sub-community identifier.
            target_peers: target number for discovery
            discovery_params: Dictionary with parameters for the discovery process
        """
        pass


class RandomWalkDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    def discover(
        self,
        subcom: "IPv8SubCommunity",
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        if self.ipv8:
            discovery = (
                (RandomWalk(subcom, **discovery_params), target_peers)
                if discovery_params
                else (RandomWalk(subcom), target_peers)
            )
            self.ipv8.strategies.append(discovery)


class EdgeWalkDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    def discover(
        self,
        subcom: "IPv8SubCommunity",
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        discovery = (
            (EdgeWalk(subcom, **discovery_params), target_peers)
            if discovery_params
            else (EdgeWalk(subcom), target_peers)
        )
        self.ipv8.strategies.append(discovery)


class NoSubCommunityDiscovery(SubCommunityDiscoveryStrategy):
    def discover(
        self,
        subcom: "BaseSubCommunity",
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        pass


class BootstrapServersDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    @abstractmethod
    def get_bootstrap_servers(self, subcom_id: bytes) -> Iterable[Peer]:
        pass

    def discover(
        self,
        subcom: "IPv8SubCommunity",
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        for k in self.get_bootstrap_servers(subcom.subcom_id):
            subcom.walk_to(k)
