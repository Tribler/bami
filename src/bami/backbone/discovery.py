from abc import ABC, ABCMeta, abstractmethod
from typing import Dict, Any, Iterable

from bami.backbone.community_routines import CommunityRoutines
from bami.backbone.sub_community import BaseSubCommunity, IPv8SubCommunity

from ipv8.peer import Peer
from ipv8.peerdiscovery.discovery import RandomWalk, EdgeWalk


class SubCommunityDiscoveryStrategy(ABC):
    @abstractmethod
    def discover(
        self,
        subcom: BaseSubCommunity,
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


class RandomWalkDiscoveryStrategy(SubCommunityDiscoveryStrategy, metaclass=ABCMeta):
    def discover(
        self,
        subcom: IPv8SubCommunity,
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        discovery = (
            (RandomWalk(subcom, **discovery_params), target_peers)
            if discovery_params
            else (RandomWalk(subcom), target_peers)
        )
        self.ipv8.strategies.append(discovery)


class EdgeWalkDiscoveryStrategy(
    SubCommunityDiscoveryStrategy, CommunityRoutines, metaclass=ABCMeta
):
    def discover(
        self,
        subcom: IPv8SubCommunity,
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
        subcom: BaseSubCommunity,
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
        subcom: IPv8SubCommunity,
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        for k in self.get_bootstrap_servers(subcom.subcom_id):
            subcom.walk_to(k)
