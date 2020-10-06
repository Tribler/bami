from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, Optional, Type, Union

from ipv8.community import Community
from ipv8.peer import Peer

from bami.backbone.community_routines import CommunityRoutines
from bami.backbone.discovery import SubCommunityDiscoveryStrategy
from bami.backbone.exceptions import UnavailableIPv8Exception


class BaseSubCommunity(ABC):
    @property
    @abstractmethod
    def subcom_id(self) -> bytes:
        pass

    @abstractmethod
    def get_known_peers(self) -> Iterable[Peer]:
        """
        Get all peers known to be in this sub-community
        Returns: list of known peers in the sub-community
        """
        pass

    @abstractmethod
    def add_peer(self, peer: Peer):
        pass

    @abstractmethod
    async def unload(self):
        pass


class IPv8SubCommunity(Community, BaseSubCommunity):
    def get_known_peers(self) -> Iterable[Peer]:
        return self.get_peers()

    @property
    def subcom_id(self) -> bytes:
        return self._subcom_id

    def __init__(self, *args, **kwargs):
        self._subcom_id = kwargs.pop("subcom_id")
        self.master_peer = Peer(self.subcom_id)
        self._prefix = b"\x00" + self.version + self.master_peer.mid
        super().__init__(*args, **kwargs)

    def add_peer(self, peer: Peer):
        self.network.add_verified_peer(peer)
        self.network.discover_services(peer, [self.master_peer.mid])


class LightSubCommunity(BaseSubCommunity):
    async def unload(self):
        pass

    def __init__(self, subcom_id: bytes = None, max_peers: int = None):
        self._subcom_id = subcom_id
        self.peers = set()

    @property
    def subcom_id(self) -> bytes:
        return self._subcom_id

    def get_known_peers(self) -> Iterable[Peer]:
        return self.peers

    def add_peer(self, peer: Peer):
        self.peers.add(peer)


class BaseSubCommunityFactory(ABC):
    @abstractmethod
    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        pass


class IPv8SubCommunityFactory(BaseSubCommunityFactory, metaclass=ABCMeta):
    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        """
        Args:
            subcom_id: id of the community
            max_peers: maximum number of peer to connect to in the community
        Returns:
            SubCommunity as an IPv8 community
        """
        if not self.ipv8:
            raise UnavailableIPv8Exception("Cannot create subcommunity without IPv8")
        else:
            subcom = IPv8SubCommunity(
                self.my_peer, self.ipv8.endpoint, self.network, *args, **kwargs
            )
            self.ipv8.overlays.append(subcom)
            return subcom


class LightSubCommunityFactory(BaseSubCommunityFactory):
    @staticmethod
    def create_subcom(*args, **kwargs) -> BaseSubCommunity:
        """
        Args:
            subcom_id: id of the community
        Returns:
            SubCommunity as a LightCommunity (just set of peers)
        """
        return LightSubCommunity(*args, **kwargs)


class SubCommunityRoutines(ABC):
    @property
    @abstractmethod
    def my_subcoms(self) -> Iterable[bytes]:
        """
        All sub-communities that my peer is part of
        Returns: list with sub-community ids
        """
        pass

    @abstractmethod
    def discovered_peers_by_subcom(self, subcom_id) -> Iterable[Peer]:
        pass

    @abstractmethod
    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    @abstractmethod
    def add_subcom(self, sub_com: bytes, subcom_obj: BaseSubCommunity) -> None:
        """
        Add sub-community to the community object.
        Args:
            sub_com: sub-community identifier
            subcom_obj: SubCommunity object
        """
        pass

    @abstractmethod
    def notify_peers_on_new_subcoms(self) -> None:
        """Notify other peers on updates of the sub-communities"""
        pass

    @property
    @abstractmethod
    def subcom_factory(
        self,
    ) -> Union[BaseSubCommunityFactory, Type[BaseSubCommunityFactory]]:
        """Factory for creating sub-communities"""
        pass

    @abstractmethod
    def on_join_subcommunity(self, sub_com_id: bytes) -> None:
        """
        Join to the gossip process for the sub-community
        Args:
            sub_com_id: sub-community identifier
        """
        pass


class SubCommunityMixin(SubCommunityRoutines, CommunityRoutines, metaclass=ABCMeta):
    def is_subscribed(self, community_id: bytes) -> bool:
        return community_id in self.my_subcoms

    def join_subcom(
        self, subcom_id: bytes, discovery_params: Dict[str, Any] = None
    ) -> None:
        if subcom_id not in self.my_subcoms:
            # This sub-community is still not known
            # Set max peers setting
            subcom = self.subcom_factory.create_subcom(
                subcom_id=subcom_id, max_peers=self.settings.subcom_max_peers
            )
            # Add the sub-community to the main overlay
            self.add_subcom(subcom_id, subcom)
            # Call discovery routine for this sub-community
            for p in self.discovered_peers_by_subcom(subcom_id):
                subcom.add_peer(p)
            self.discovery_strategy.discover(
                subcom,
                target_peers=self.settings.subcom_min_peers,
                discovery_params=discovery_params,
            )

    def subscribe_to_subcoms(
        self, subcoms: Iterable[bytes], discovery_params: Dict[str, Any] = None
    ) -> None:
        """
        Subscribe to the sub communities with given ids.

        If bootstrap_master is not specified, we will use RandomWalks to discover other peers within the same community.
        A peer will connect to at most `settings.max_peers_subtrust` peers.
        Args:
            subcoms: Iterable object with sub_community ids
            discovery_params: Dict parameters for the discovery process
        """
        updated = False
        for c_id in subcoms:
            if c_id not in self.my_subcoms:
                self.join_subcom(c_id, discovery_params)
                # Join the sub-community
                self.on_join_subcommunity(c_id)
                updated = True
        if updated:
            self.notify_peers_on_new_subcoms()

    def subscribe_to_subcom(
        self, subcom_id: bytes, discovery_params: Dict[str, Any] = None
    ) -> None:
        """
        Subscribe to the SubCommunity with the public key master peer.
        Community is identified with a community_id.

        Args:
            subcom_id: bytes identifier of the community
            discovery_params: Dict parameters for the discovery process
        """
        if subcom_id not in self.my_subcoms:
            self.join_subcom(subcom_id, discovery_params)

            # Join the protocol audits/ updates
            self.on_join_subcommunity(subcom_id)

            # Notify other peers that you are part of the new community
            self.notify_peers_on_new_subcoms()
