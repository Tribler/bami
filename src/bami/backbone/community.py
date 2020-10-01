"""
The Plexus backbone
"""
from abc import ABCMeta, abstractmethod
from asyncio import (
    ensure_future,
    Future,
    iscoroutinefunction,
    Queue,
    sleep,
    Task,
)
from binascii import hexlify, unhexlify
from enum import Enum
import logging
import random
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union

from bami.backbone.block import BamiBlock
from bami.backbone.block_sync import BlockSyncMixin
from bami.backbone.community_routines import MessageStateMachine
from bami.backbone.datastore.block_store import LMDBLockStore
from bami.backbone.datastore.chain_store import ChainFactory
from bami.backbone.datastore.database import BaseDB, ChainTopic, DBManager
from bami.backbone.datastore.frontiers import Frontier
from bami.backbone.exceptions import (
    DatabaseDesynchronizedException,
    InvalidTransactionFormatException,
    IPv8UnavailableException,
    SubCommunityEmptyException,
    UnknownChainException,
)
from bami.backbone.gossip import SubComGossipMixin
from bami.backbone.payload import SubscriptionsPayload
from bami.backbone.settings import BamiSettings
from bami.backbone.sub_community import (
    BaseSubCommunity,
    BaseSubCommunityFactory,
    SubCommunityDiscoveryStrategy,
    SubCommunityMixin,
)
from bami.backbone.utils import (
    CONFIRM_TYPE,
    decode_raw,
    Dot,
    EMPTY_PK,
    encode_raw,
    Links,
    Notifier,
    REJECT_TYPE,
    shorten,
    WITNESS_TYPE,
)
from ipv8.community import Community
from ipv8.keyvault.keys import Key
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from ipv8.peerdiscovery.discovery import EdgeWalk, RandomWalk
from ipv8.peerdiscovery.network import Network
from ipv8.util import coroutine
from ipv8_service import IPv8


class BlockResponse(Enum):
    CONFIRM = 1
    REJECT = 2
    DELAY = 3


class BamiCommunity(
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

    async def flex_runner(
        self,
        delay: Callable[[], float],
        interval: Callable[[], float],
        task: Callable,
        *args: List
    ) -> None:
        await sleep(delay())
        while True:
            await task(*args)
            await sleep(interval())

    def register_flexible_task(
        self,
        name: str,
        task: Callable,
        *args: List,
        delay: Callable = None,
        interval: Callable = None
    ) -> Union[Future, Task]:
        """
        Register a Task/(coroutine)function so it can be canceled at shutdown time or by name.
        """
        if not delay:

            def delay():
                return random.random()

        if not interval:

            def interval():
                return random.random()

        task = task if iscoroutinefunction(task) else coroutine(task)
        return self.register_task(
            name, ensure_future(self.flex_runner(delay, interval, task, *args))
        )

    def __init__(
        self,
        my_peer: Peer,
        endpoint: Any,
        network: Network,
        ipv8: Optional[IPv8] = None,
        max_peers: int = None,
        anonymize: bool = False,
        db: BaseDB = None,
        work_dir: str = None,
        settings: BamiSettings = None,
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
            self._settings = BamiSettings()
        else:
            self._settings = settings

        if not work_dir:
            work_dir = self.settings.work_directory
        if not db:
            self._persistence = DBManager(ChainFactory(), LMDBLockStore(work_dir))
        else:
            self._persistence = db
        if not max_peers:
            max_peers = self.settings.main_max_peers
        self._ipv8 = ipv8
        super(BamiCommunity, self).__init__(
            my_peer, endpoint, network, max_peers, anonymize=anonymize
        )

        # Create DB Manager

        self.logger.debug(
            "The Plexus community started with Public Key: %s",
            hexlify(self.my_peer.public_key.key_to_bin()),
        )
        self.relayed_broadcasts = set()

        self.shutting_down = False

        # Sub-Communities logic
        self.my_subscriptions = dict()

        self.peer_subscriptions = (
            dict()
        )  # keeps track of which communities each peer is part of
        self.bootstrap_master = None

        self.periodic_sync_lc = {}

        self.incoming_queues = {}
        self.processing_queue_tasks = {}

        self.ordered_notifier = Notifier()
        self.unordered_notifier = Notifier()

        # Setup and add message handlers
        for base in BamiCommunity.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

        self.add_message_handler(SubscriptionsPayload, self.received_peer_subs)

    # ----- Discovery start -----
    def start_discovery(
        self,
        target_peers: int = None,
        discovery_algorithm: Union[Type[RandomWalk], Type[EdgeWalk]] = RandomWalk,
        discovery_params: Dict[str, Any] = None,
    ):

        if not self._ipv8:
            raise IPv8UnavailableException("Cannot start discovery at main community")

        discovery = (
            discovery_algorithm(self)
            if not discovery_params
            else discovery_algorithm(self, **discovery_params)
        )
        if not target_peers:
            target_peers = self.settings.main_min_peers
        self._ipv8.add_strategy(self, discovery, target_peers)

    # ----- Update notifiers for new blocks ------------

    def get_block_and_blob_by_dot(
        self, chain_id: bytes, dot: Dot
    ) -> Tuple[bytes, BamiBlock]:
        """Get blob and serialized block and by the chain_id and dot.
        Can raise DatabaseDesynchronizedException if no block found."""
        blk_blob = self.persistence.get_block_blob_by_dot(chain_id, dot)
        if not blk_blob:
            raise DatabaseDesynchronizedException(
                "Block is not found in db: {chain_id}, {dot}".format(
                    chain_id=chain_id, dot=dot
                )
            )
        block = BamiBlock.unpack(blk_blob, self.serializer)
        return blk_blob, block

    def get_block_by_dot(self, chain_id: bytes, dot: Dot) -> BamiBlock:
        """Get block by the chain_id and dot. Can raise DatabaseDesynchronizedException"""
        return self.get_block_and_blob_by_dot(chain_id, dot)[1]

    def block_notify(self, chain_id: bytes, dots: List[Dot]):
        self.logger.info("Processing dots %s on chain: %s", dots, chain_id)
        for dot in dots:
            block = self.get_block_by_dot(chain_id, dot)
            self.ordered_notifier.notify(chain_id, block)

    def subscribe_in_order_block(
        self, topic: Union[bytes, ChainTopic], callback: Callable[[BamiBlock], None]
    ):
        """Subscribe on block updates received in-order. Callable will receive the block."""
        self._persistence.add_unique_observer(topic, self.block_notify)
        self.ordered_notifier.add_observer(topic, callback)

    def subscribe_out_order_block(
        self, topic: Union[bytes, ChainTopic], callback: Callable[[BamiBlock], None]
    ):
        """Subscribe on block updates received in-order. Callable will receive the block."""
        self.unordered_notifier.add_observer(topic, callback)

    def process_block_unordered(self, blk: BamiBlock, peer: Peer) -> None:
        self.unordered_notifier.notify(blk.com_prefix + blk.com_id, blk)
        if peer != self.my_peer:
            frontier = Frontier(Links((blk.com_dot,)), holes=(), inconsistencies=())
            subcom_id = blk.com_prefix + blk.com_id
            processing_queue = self.incoming_frontier_queue(subcom_id)
            if not processing_queue:
                raise UnknownChainException(
                    "Cannot process block received block with unknown chain. {subcom_id}".format(
                        subcom_id=subcom_id
                    )
                )
            processing_queue.put_nowait((peer, frontier, True))

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
        for mid in self.processing_queue_tasks:
            if not self.processing_queue_tasks[mid].done():
                self.processing_queue_tasks[mid].cancel()
        for subcom_id in self.my_subscriptions:
            await self.my_subscriptions[subcom_id].unload()
        await super(BamiCommunity, self).unload()

        # Close the persistence layer
        self.persistence.close()

    @property
    def settings(self) -> BamiSettings:
        return self._settings

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

    def start_gossip_sync(
        self,
        subcom_id: bytes,
        prefix: bytes = b"",
        delay: Callable[[], float] = None,
        interval: Callable[[], float] = None,
    ) -> None:
        full_com_id = prefix + subcom_id
        self.logger.debug("Starting gossip with frontiers on chain %s", full_com_id)
        self.periodic_sync_lc[full_com_id] = self.register_flexible_task(
            "gossip_sync_" + str(full_com_id),
            self.gossip_sync_task,
            subcom_id,
            prefix,
            delay=delay
            if delay
            else lambda: random.random() * self._settings.gossip_sync_max_delay,
            interval=interval if interval else lambda: self._settings.gossip_interval,
        )
        self.incoming_queues[full_com_id] = Queue()
        self.processing_queue_tasks[full_com_id] = ensure_future(
            self.process_frontier_queue(full_com_id)
        )

    def incoming_frontier_queue(self, subcom_id: bytes) -> Optional[Queue]:
        return self.incoming_queues.get(subcom_id)

    def get_peer_by_key(
        self, peer_key: bytes, subcom_id: bytes = None
    ) -> Optional[Peer]:
        if subcom_id:
            subcom_peers = self.get_subcom(subcom_id).get_known_peers()
            for peer in subcom_peers:
                if peer.public_key.key_to_bin() == peer_key:
                    return peer
        for peer in self.get_peers():
            if peer.public_key.key_to_bin() == peer_key:
                return peer
        return None

    def choose_community_peers(
        self, com_peers: Iterable[Peer], current_seed: Any, commitee_size: int
    ) -> Iterable[Peer]:
        rand = random.Random(current_seed)
        return rand.sample(com_peers, min(commitee_size, len(com_peers)))

    def share_in_community(
        self,
        block: Union[BamiBlock, bytes],
        subcom_id: bytes = None,
        ttl: int = None,
        fanout: int = None,
        seed: Any = None,
    ) -> None:
        """
        Share a block with peers in a sub-community via push-based gossip.
        Args:
            block: the BamiBlock to share, either as BamiBlock instance or in serialized form
            subcom_id: identity of the sub-community, if not specified the main community connections will be used.
            ttl: ttl of the gossip, if not specified the default settings will be used
            fanout: of the gossip, if not specified the default settings will be used
            seed: seed for the peers selection, if not specified a random value will be used
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
    def apply_witness_tx(self, block: BamiBlock, witness_tx: Any) -> None:
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
                    block_type=WITNESS_TYPE,
                    transaction=witness_blob,
                    prefix=b"w",
                    com_id=chain_id,
                    use_consistent_links=False,
                )
                self.logger.debug(
                    "Creating witness block on chain %s: %s, com_dot %s, pers_dot %s",
                    shorten(blk.com_id),
                    seq_num,
                    blk.com_dot,
                    blk.pers_dot,
                )
                self.share_in_community(blk, chain_id)

    def process_witness(self, block: BamiBlock) -> None:
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
    def confirm(self, block: BamiBlock, extra_data: Dict = None) -> None:
        """
        Confirm the transaction in an incoming block. Link will be in the transaction with block dot.
        Args:
            block: The BamiBlock to confirm.
            extra_data: An optional dictionary with extra data that is appended to the confirmation.
        """
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        confirm_tx = {b"initiator": block.public_key, b"dot": dot}
        if extra_data:
            confirm_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=CONFIRM_TYPE, transaction=encode_raw(confirm_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def verify_confirm_tx(self, claimer: bytes, confirm_tx: Dict) -> None:
        # 1. verify claim format
        if not confirm_tx.get(b"initiator") or not confirm_tx.get(b"dot"):
            raise InvalidTransactionFormatException(
                "Invalid claim ", claimer, confirm_tx
            )

    def process_confirm(self, block: BamiBlock) -> None:
        confirm_tx = decode_raw(block.transaction)
        self.verify_confirm_tx(block.public_key, confirm_tx)
        self.apply_confirm_tx(block, confirm_tx)

    @abstractmethod
    def apply_confirm_tx(self, block: BamiBlock, confirm_tx: Dict) -> None:
        pass

    def reject(self, block: BamiBlock, extra_data: Dict = None) -> None:
        """
        Reject the transaction in an incoming block.

        Args:
            block: The BamiBlock to reject.
            extra_data: Some additional data to append to the reject transaction, e.g., a reason.
        """
        # change it to confirm
        # create claim block and share in the community
        chain_id = block.com_id if block.com_id != EMPTY_PK else block.public_key
        dot = block.com_dot if block.com_id != EMPTY_PK else block.pers_dot
        reject_tx = {b"initiator": block.public_key, b"dot": dot}
        if extra_data:
            reject_tx.update(extra_data)
        block = self.create_signed_block(
            block_type=REJECT_TYPE, transaction=encode_raw(reject_tx), com_id=chain_id
        )
        self.share_in_community(block, chain_id)

    def verify_reject_tx(self, rejector: bytes, confirm_tx: Dict) -> None:
        # 1. verify reject format
        if not confirm_tx.get(b"initiator") or not confirm_tx.get(b"dot"):
            raise InvalidTransactionFormatException(
                "Invalid reject ", rejector, confirm_tx
            )

    def process_reject(self, block: BamiBlock) -> None:
        reject_tx = decode_raw(block.transaction)
        self.verify_reject_tx(block.public_key, reject_tx)
        self.apply_reject_tx(block, reject_tx)

    @abstractmethod
    def apply_reject_tx(self, block: BamiBlock, reject_tx: Dict) -> None:
        pass

    @abstractmethod
    def block_response(
        self, block: BamiBlock, wait_time: float = None, wait_blocks: int = None
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


class BamiTestnetCommunity(BamiCommunity, metaclass=ABCMeta):
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
