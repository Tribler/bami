"""
The Plexus backbone
"""
from abc import ABCMeta, abstractmethod
from binascii import hexlify, unhexlify
from enum import Enum
import logging
import random
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional, Type, Union

from ipv8.community import Community, DEFAULT_MAX_PEERS
from ipv8.keyvault.keys import Key
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from ipv8_service import IPv8

from python_project.backbone.block import PlexusBlock
from python_project.backbone.block_sync import BlockSyncMixin
from python_project.backbone.community_routines import MessageStateMachine
from python_project.backbone.datastore.block_store import LMDBLockStore
from python_project.backbone.datastore.chain_store import ChainFactory
from python_project.backbone.datastore.database import BaseDB, DBManager
from python_project.backbone.utils import decode_raw, EMPTY_PK, encode_raw
from python_project.backbone.exceptions import (
    InvalidTransactionFormatException,
    SubCommunityEmptyException,
)
from python_project.backbone.gossip import SubComGossipMixin
from python_project.backbone.payload import SubscriptionsPayload
from python_project.backbone.settings import PlexusSettings
from python_project.backbone.sub_community import (
    BaseSubCommunity,
    BaseSubCommunityFactory,
    SubCommunityDiscoveryStrategy,
    SubCommunityMixin,
)

WITNESS_TYPE = b"witness"
CONFIRM_TYPE = b"confirm"
REJECT_TYPE = b"reject"


class BlockResponse(Enum):
    CONFIRM = 1
    REJECT = 2
    DELAY = 3


class PlexusCommunity(
    Community,
    BlockSyncMixin,
    SubComGossipMixin,
    SubCommunityMixin,
    BaseSubCommunityFactory,
    SubCommunityDiscoveryStrategy,
    metaclass=ABCMeta,
):
    """
    Community for secure backbone.
    """

    master_peer = Peer(
        unhexlify(
            "4c69624e61434c504b3a062780beaeb40e70fca4cfc1b7751d734f361cf8d815db24dbb8a99fc98af4"
            "39fc977d84f71a431f8825ba885a5cf86b2498c6b473f33dd20dbdcffd199048fc"
        )
    )
    version = b"\x02"

    def __init__(
        self,
        my_peer: Peer,
        endpoint: Any,
        network: Network,
        ipv8: Optional[IPv8] = None,
        max_peers: int = DEFAULT_MAX_PEERS,
        anonymize: bool = False,
        db: BaseDB = None,
        work_dir: str = None,
        settings: PlexusSettings = None,
        **kwargs
    ):
        """

        Args:
            my_peer:
            endpoint:
            network:
            max_peers:
            anonymize:
            db:
        """
        if not settings:
            self._settings = PlexusSettings()
        else:
            self._settings = settings

        if not work_dir:
            work_dir = self.settings.work_directory
        if not db:
            self._persistence = DBManager(ChainFactory(), LMDBLockStore(work_dir))
        else:
            self._persistence = db
        self._ipv8 = ipv8
        self.receive_block_lock = RLock()
        super(PlexusCommunity, self).__init__(
            my_peer, endpoint, network, max_peers, anonymize=anonymize
        )

        self._request_cache = RequestCache()
        self._logger = logging.getLogger(self.__class__.__name__)
        # Create DB Manager

        self.logger.debug(
            "The Plexus community started with Public Key: %s",
            hexlify(self.my_peer.public_key.key_to_bin()),
        )
        self.relayed_broadcasts = set()

        self.shutting_down = False

        self.periodic_sync_lc = {}

        # Sub-Communities logic
        self.my_subscriptions = dict()

        self.peer_subscriptions = (
            dict()
        )  # keeps track of which communities each peer is part of
        self.bootstrap_master = None

        # Setup and add message handlers
        for base in PlexusCommunity.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

        self.add_message_handler(SubscriptionsPayload, self.received_peer_subs)

    # ---- Introduction handshakes => Exchange your subscriptions ----------------
    def create_introduction_request(
        self, socket_address: Any, extra_bytes: bytes = b""
    ):
        extra_bytes = encode_raw(self.my_subcoms)
        return super().create_introduction_request(socket_address, extra_bytes)

    def create_introduction_response(
        self,
        lan_socket_address,
        socket_address,
        identifier,
        introduction=None,
        extra_bytes=b"",
        prefix=None,
    ):
        extra_bytes = encode_raw(self.my_subcoms)
        return super().create_introduction_response(
            lan_socket_address,
            socket_address,
            identifier,
            introduction,
            extra_bytes,
            prefix,
        )

    def introduction_response_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)
        # TODO: add subscription strategy
        if self.settings.track_neighbours_chains:
            self.subscribe_to_subcom(peer.public_key.key_to_bin())

    def introduction_request_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)
        # TODO: add subscription strategy
        if self.settings.track_neighbours_chains:
            self.subscribe_to_subcom(peer.public_key.key_to_bin())

    # ----- Community routines ------

    async def unload(self):
        self.logger.debug("Unloading the Plexus Community.")
        self.shutting_down = True

        await self.request_cache.shutdown()
        for mid in self.my_subscriptions:
            if mid in self.periodic_sync_lc and not self.periodic_sync_lc[mid].done():
                self.periodic_sync_lc[mid].cancel()
        await super(PlexusCommunity, self).unload()
        # Close the persistence layer
        self.persistence.close()

    @property
    def settings(self) -> Any:
        return self._settings

    @property
    def request_cache(self) -> RequestCache:
        return self._request_cache

    @property
    def persistence(self) -> BaseDB:
        return self._persistence

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def send_packet(self, peer: Peer, packet: Any, sig: bool = True) -> None:
        self.ez_send(peer, packet, sig=sig)

    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    # ----- SubCommunity routines ------
    def get_subcom_discovery_strategy(
        self, subcom_id: bytes
    ) -> Union[SubCommunityDiscoveryStrategy, Type[SubCommunityDiscoveryStrategy]]:
        return self

    @property
    def subcom_factory(
        self,
    ) -> Union[BaseSubCommunityFactory, Type[BaseSubCommunityFactory]]:
        return self

    @property
    def my_subcoms(self) -> Iterable[bytes]:
        return list(self.my_subscriptions.keys())

    def get_subcom(self, subcom_id: bytes) -> Optional[BaseSubCommunity]:
        return self.my_subscriptions.get(subcom_id)

    def add_subcom(self, sub_com: bytes, subcom_obj: BaseSubCommunity) -> None:
        if not subcom_obj:
            raise SubCommunityEmptyException("Sub-Community object is none", sub_com)
        self.my_subscriptions[sub_com] = subcom_obj

    def discovered_peers_by_subcom(self, subcom_id: bytes) -> Iterable[Peer]:
        return self.peer_subscriptions.get(subcom_id, [])

    def process_peer_subscriptions(self, peer: Peer, subcoms: List[bytes]) -> None:
        for c in subcoms:
            # For each sub-community that is also known to me - introduce peer.
            if c in self.my_subscriptions:
                self.my_subscriptions[c].add_peer(peer)
            # Keep all sub-communities and peer in a map
            if c not in self.peer_subscriptions:
                self.peer_subscriptions[c] = set()
            self.peer_subscriptions[c].add(peer)

    @lazy_wrapper(SubscriptionsPayload)
    def received_peer_subs(self, peer: Peer, payload: SubscriptionsPayload) -> None:
        subcoms = decode_raw(payload.subcoms)
        self.process_peer_subscriptions(peer, subcoms)

    def notify_peers_on_new_subcoms(self) -> None:
        for peer in self.get_peers():
            self.send_packet(
                peer,
                SubscriptionsPayload(self.my_pub_key_bin, encode_raw(self.my_subcoms)),
            )

    # -------- Community block sharing  -------------

    def choose_community_peers(
        self, com_peers: Iterable[Peer], current_seed: Any, commitee_size: int
    ) -> Iterable[Peer]:
        rand = random.Random(current_seed)
        return rand.sample(com_peers, min(commitee_size, len(com_peers)))

    def share_in_community(
        self,
        block: Union[PlexusBlock, bytes],
        subcom_id: bytes = None,
        ttl: int = None,
        fanout: int = None,
        seed: Any = None,
    ) -> None:
        """
        Share block in sub-community via push-based gossip.
        Args:
            block: PlexusBlock to share
            subcom_id: identity of the sub-community, if not specified the main community connections will be used.
            ttl: ttl of the gossip, if not specified - default settings will be used
            fanout: of the gossip, if not specified - default settings will be used
            seed: seed for the peers selection, otherwise random value will be used
        """
        if not subcom_id or not self.get_subcom(subcom_id):
            subcom_peers = self.get_peers()
        else:
            subcom_peers = self.get_subcom(subcom_id).get_known_peers()
        if not seed:
            seed = random.random()
        if not fanout:
            fanout = self.settings.push_gossip_fanout
        if not ttl:
            ttl = self.settings.push_gossip_ttl
        if subcom_peers:
            selected_peers = self.choose_community_peers(subcom_peers, seed, fanout)
            self.send_block(block, selected_peers, ttl)

    # ------ Audits for the chain wrp to invariants -----
    @abstractmethod
    def witness_tx_well_formatted(self, witness_tx: Any) -> bool:
        """
        Returns:
            False if bad format
        """
        pass

    @abstractmethod
    def build_witness_blob(self, chain_id: bytes, seq_num: int) -> Optional[bytes]:
        """
        Args:
            chain_id: bytes identifier of the chain
            seq_num: of the chain to audit to
        Returns:
            witness blob (bytes) if possible, None otherwise
        """
        pass

    @abstractmethod
    def apply_witness_tx(self, block: PlexusBlock, witness_tx: Any) -> None:
        pass

    def verify_witness_transaction(self, chain_id: bytes, witness_tx: Any) -> None:
        """
        Verify the witness transaction
        Raises:
             InvalidFormatTransaction
        """
        # 1. Witness transaction ill-formatted
        if not self.witness_tx_well_formatted(witness_tx):
            raise InvalidTransactionFormatException(
                "Invalid witness transaction", chain_id, witness_tx
            )

    def witness(self, chain_id: bytes, seq_num: int) -> None:
        """
        Witness the chain up to a sequence number.
        If chain is known and data exists:
         - Will create a witness block, link to latest known blocks and share in the community.
        Otherwise:
         - Do nothing
        Args:
            chain_id: id of the chain
            seq_num: sequence number of the block:
        """
        chain = self.persistence.get_chain(chain_id)
        if chain:
            witness_blob = self.build_witness_blob(chain_id, seq_num)
            if witness_blob:
                blk = self.create_signed_block(
                    block_type=WITNESS_TYPE, transaction=witness_blob, com_id=chain_id
                )
                self.share_in_community(blk, chain_id)

    def process_witness(self, block: PlexusBlock) -> None:
        """Process received witness transaction"""
        witness_tx = self.unpack_witness_blob(block.transaction)
        chain_id = block.com_id
        self.verify_witness_transaction(chain_id, witness_tx)
        # Apply to db
        self.apply_witness_tx(block, witness_tx)

    def unpack_witness_blob(self, witness_blob: bytes) -> Any:
        """
        Returns:
            decoded witness transaction
        """
        return decode_raw(witness_blob)

    # ------ Confirm and reject functions --------------
    def confirm(self, block: PlexusBlock, extra_data: Dict = None) -> None:
        """Create confirm block linked to block. Link will be in the transaction with block dot.
           Add extra data to the transaction with a 'extra_data' dictionary.
        """
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        confirm_tx = {"initiator": block.public_key, "dot": dot}
        if extra_data:
            confirm_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=CONFIRM_TYPE, transaction=encode_raw(confirm_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def verify_confirm_tx(self, claimer: bytes, confirm_tx: Dict) -> None:
        # 1. verify claim format
        if not confirm_tx.get("initiator") or not confirm_tx.get("dot"):
            raise InvalidTransactionFormatException(
                "Invalid claim ", claimer, confirm_tx
            )

    def process_confirm(self, block: PlexusBlock) -> None:
        confirm_tx = decode_raw(block.transaction)
        self.verify_confirm_tx(block.public_key, confirm_tx)
        self.apply_confirm_tx(block, confirm_tx)

    @abstractmethod
    def apply_confirm_tx(self, block: PlexusBlock, confirm_tx: Dict) -> None:
        pass

    def reject(self, block: PlexusBlock, extra_data: Dict = None) -> None:
        # change it to confirm
        # create claim block and share in the community
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        reject_tx = {"initiator": block.public_key, "dot": dot}
        if extra_data:
            reject_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=REJECT_TYPE, transaction=encode_raw(reject_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def verify_reject_tx(self, rejector: bytes, confirm_tx: Dict) -> None:
        # 1. verify reject format
        if not confirm_tx.get("initiator") or not confirm_tx.get("dot"):
            raise InvalidTransactionFormatException(
                "Invalid reject ", rejector, confirm_tx
            )

    def process_reject(self, block: PlexusBlock) -> None:
        reject_tx = decode_raw(block.transaction)
        self.verify_reject_tx(block.public_key, reject_tx)
        self.apply_reject_tx(block, reject_tx)

    @abstractmethod
    def apply_reject_tx(self, block: PlexusBlock, reject_tx: Dict) -> None:
        pass

    @abstractmethod
    def block_response(
        self, block: PlexusBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        """
        Respond to block BlockResponse: Reject, Confirm, Delay
        Args:
            block: to respond to
            wait_time: time that passed since first block process initiated
            wait_blocks: number of blocks passed since the block
        Returns:
            BlockResponse: Confirm, Reject or Delay
        """
        pass

    # ----- Request state accumulation for the chain -----

    # ----- When receive block - analyze if you need to respond for the block. ----

    # ----------- Auditing chain state wrp invariants ----------------


class PlexusTestnetCommunity(PlexusCommunity, metaclass=ABCMeta):
    """
    This community defines the testnet for Plexus
    """

    DB_NAME = "plexus_testnet"

    master_peer = Peer(
        unhexlify(
            "4c69624e61434c504b3abaa09505b032231182217276fc355dc38fb8e4998a02f91d3ba00f6fbf648"
            "5116b8c8c212be783fc3171a529f50ce25feb6c4dcc8106f468e5401bf37e8129e2"
        )
    )
