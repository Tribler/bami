"""
The Plexus backbone
"""
import logging
import random
from asyncio import Queue, ensure_future, sleep
from binascii import hexlify, unhexlify
from collections import defaultdict
from collections import deque
from functools import wraps
from threading import RLock
from typing import Any, Iterable, Optional, Union, Type

import orjson as json
from ipv8.community import Community
from ipv8.keyvault.keys import Key
from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.messaging.payload_headers import GlobalTimeDistributionPayload
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from ipv8.util import succeed
from python_project.backbone.block import PlexusBlock
from python_project.backbone.consts import *
from python_project.backbone.datastore.block_store import LMDBLockStore
from python_project.backbone.datastore.chain_store import ChainFactory
from python_project.backbone.datastore.database import DBManager, BaseDB, ChainTopic
from python_project.backbone.datastore.state_store import State
from python_project.backbone.datastore.utils import (
    encode_raw,
    decode_raw,
)
from python_project.backbone.listener import BlockListener
from python_project.backbone.payload import *
from python_project.backbone.settings import PlexusSettings
from python_project.backbone.sub_community import SubCommunityMixin, BaseSubCommunityFactory, \
    SubCommunityDiscoveryStrategy, BaseSubCommunity
from python_project.backbone.gossip import GossipFrontiersMixin, NextPeerSelectionStrategy


def synchronized(f):
    """
    Due to database inconsistencies, we can't allow multiple threads to handle a received_block at the same time.
    """

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        with self.receive_block_lock:
            return f(self, *args, **kwargs)

    return wrapper


class PlexusBlockListener(BlockListener):
    """
    This block listener simply signs all blocks it receives.
    """

    BLOCK_CLASS = PlexusBlock

    def should_sign(self, block):
        return True

    def received_block(self, block):
        pass


class IntroductionMixin:

    # ---- Introduction handshakes => Exchange your subscriptions ----------------
    def create_introduction_request(
            self, socket_address: Any, extra_bytes: bytes = b""
    ):
        extra_bytes = self.encode_subcom(self.my_subcoms)
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
        extra_bytes = self.encode_subcom(self.my_subcoms)
        return super(PlexusCommunity, self).create_introduction_response(
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
        communities = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, communities)
        # TODO: add subscription strategy
        if self.settings.track_neighbours_chains:
            self.subscribe_to_subcom(peer.public_key.key_to_bin())


class PlexusCommunity(Community, IntroductionMixin, SubCommunityMixin, GossipFrontiersMixin):
    """
    Community for secure backbone.
    """

    master_peer = Peer(
        unhexlify(
            "4c69624e61434c504b3a062780beaeb40e70fca4cfc1b7751d734f361cf8d815db24dbb8a99fc98af4"
            "39fc977d84f71a431f8825ba885a5cf86b2498c6b473f33dd20dbdcffd199048fc"
        )
    )

    UNIVERSAL_BLOCK_LISTENER = b"UNIVERSAL_BLOCK_LISTENER"
    DB_NAME = "plexus"
    version = b"\x02"

    def __init__(self, *args, **kwargs):

        # initialize the community database

        working_directory = kwargs.pop("working_directory", ".")
        db_name = kwargs.pop("db_name", self.DB_NAME)
        self.settings = kwargs.pop("settings", PlexusSettings())

        # TODO: Change it to dependency injection

        self._persistence = DBManager(ChainFactory(), LMDBLockStore(working_directory))
        self.ipv8 = kwargs.pop("ipv8", None)

        self.receive_block_lock = RLock()
        super(PlexusCommunity, self).__init__(*args, **kwargs)

        self.request_cache = RequestCache()
        self._logger = logging.getLogger(self.__class__.__name__)
        # Create DB Manager

        self.relayed_broadcasts = set()
        self.relayed_broadcasts_order = deque()
        self.logger.debug(
            "The Plexus community started with Public Key: %s",
            hexlify(self.my_peer.public_key.key_to_bin()),
        )
        self.shutting_down = False
        self.listeners_map = {}  # Map of block_type -> [callbacks]

        self.periodic_sync_lc = {}
        # self.operation_queue = Queue()
        # self.operation_queue_task

        # Block queues
        self.incoming_block_queue = Queue()
        self.incoming_block_queue_task = ensure_future(
            self.evaluate_incoming_block_queue()
        )

        self.outgoing_block_queue = Queue()
        self.outgoing_block_queue_task = ensure_future(
            self.evaluate_outgoing_block_queue()
        )

        self.audit_response_queue = Queue()
        # self.audit_response_queue_task = ensure_future(self.evaluate_audit_response_queue())

        self.mem_db_flush_lc = None
        self.transaction_lc = None

        # Communities logic
        self.interest = dict()
        self.my_subscriptions = dict()

        self._states = dict()

        self.peer_subscriptions = (
            dict()
        )  # keeps track of which communities each peer is part of
        self.bootstrap_master = None
        self.proof_requests = {}

        self.add_message_handler(BlockPayload, self.received_block)
        self.add_message_handler(SubscriptionsPayload, self.received_peer_subs)
        self.add_message_handler(RawBlockPayload, self.received_raw_block)

        self.decode_map.update(
            {
                chr(BLOCKS_REQ_MSG): self.received_blocks_request,
                chr(BLOCK_CAST_MSG): self.received_block_broadcast,
                chr(FRONTIER_MSG): self.received_frontier,
                chr(STATE_REQ_MSG): self.received_state_request,
                chr(STATE_RESP_MSG): self.received_state_response,
                chr(STATE_BY_HASH_REQ_MSG): self.received_state_by_hash_request,
                chr(STATE_BY_HASH_RESP_MSG): self.received_state_by_hash_response,
            }
        )

    @property
    def persistence(self) -> BaseDB:
        return self._persistence

    def get_ipv8(self) -> Optional[Any]:
        return self.ipv8

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def my_pub_key(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def send_packet(self, peer: Peer, packet: Any, sig: bool = True) -> None:
        self.ez_send(peer, packet, sig=sig)

    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    def add_state_db(self, state: State, chain_topic: ChainTopic) -> None:
        if chain_topic not in self._states:
            self._states[chain_topic] = set()
        self._states[chain_topic].add(state)

    # ----- SubCommunity routines ------
    @property
    def my_subcoms(self) -> Iterable[bytes]:
        return list(self.my_subscriptions)

    def encode_subcom(self, subcom: Iterable[bytes]) -> bytes:
        return encode_raw(self.my_subcoms)

    def add_subcom(
            self, subcom_id: bytes, subcom_obj: Optional[SubCommunity] = None
    ) -> None:
        self.my_subscriptions[subcom_id] = subcom_obj

    def get_subcom_notify_peers(self) -> Iterable[Peer]:
        return self.get_peers()

    def process_peer_subscriptions(
            self, peer: Peer, communities: Iterable[bytes]
    ) -> None:
        for c in communities:
            if c not in self.peer_subscriptions:
                self.peer_subscriptions[c] = set()
            self.peer_subscriptions[c].add(peer)

    @lazy_wrapper(SubscriptionsPayload)
    def received_peer_subs(self, peer: Peer, payload: SubscriptionsPayload) -> None:
        subcoms = decode_raw(payload.subcoms)
        self.process_peer_subscriptions(peer, subcoms)

    def join_subcommunity_gossip(self, sub_com_id: bytes):
        """
        Periodically exchange latest information in the community.
        There are two possibilities:
        1. Gossip protocol with a log reconciliation. [Integrity Violation Detection] SecurityMode.VANILLA
            Latest transaction will be shared and compared with random peers in the community.
            As other peers are repeating the same, this ensures that if information will be known by all peer eventually.
            As all information is applied in a consistent way, an integrity violation will be detected.
        2. Active auditing request with witnesses. [Probabilistic Violation Prevention] SecurityMode.AUDIT
            If community requires prevention of certain violation that can be guaranteed with probability (1-epsilon).
            Epsilon depends on multiple parameters, but the main one: fraction of malicious peers in the community.
        Periodically gossip latest information to the community.

        Args:
            sub_com_id: identifier for the sub-community
        """
        # Start sync task after the discovery
        # TODO: Use GossipStrategies
        pass

        # task = self.gossip_sync_task if mode == SecurityMode.VANILLA else None

        # self.periodic_sync_lc[community_mid] = self.register_task(
        #    "sync_" + str(community_mid),
        #    task,
        #    community_mid,
        #    delay=random.random(),
        #    interval=sync_time,
        # )

    # -------- Additional -------------

    """ Choice for the block broadcast
    # block public key => personal chain
            pers_subs = self.peer_subscriptions.get(block.public_key)
            if pers_subs:
                f = min(len(pers_subs), self.settings.broadcast_fanout)
                pers_subs = random.sample(pers_subs, f)
                for p in pers_subs:
                    self.outgoing_block_queue.put_nowait((p.address, packet))

            # block vertical chain subs
            if block.com_id != EMPTY_PK:
                com_subs = self.peer_subscriptions.get(block.com_id)
                if com_subs:
                    f = min(len(com_subs), self.settings.broadcast_fanout)
                    com_subs = random.sample(com_subs, f)
                    for p in com_subs:
                        self.outgoing_block_queue.put_nowait((p.address, packet))
    """

    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    def notify_peer_on_new_subcoms(self) -> None:
        pass

    def get_subcom_discovery_strategy(self, subcom_id: bytes) -> Union[
        SubCommunityDiscoveryStrategy, Type[SubCommunityDiscoveryStrategy]]:
        pass

    @property
    def subcom_factory(self) -> Union[BaseSubCommunityFactory, Type[BaseSubCommunityFactory]]:
        pass

    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        pass

    @property
    def my_peer(self) -> Peer:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        pass

    @property
    def network(self) -> Network:
        pass

    @property
    def request_cache(self) -> RequestCache:
        pass

    def get_cumulative_state_blob(self, subcom_id: bytes) -> Optional[bytes]:
        pass

    def get_state(self, subcom_id: bytes) -> Optional[State]:
        pass

    def send_block(
            self, block: Union[PlexusBlock, bytes], peers: Iterable[Peer], ttl: int = 1
    ) -> None:
        """
        Send a block to the set of peers. If ttl is higher than 1: will gossip the message further.
        Args:
            block: block to send
            peers: set of peers
            ttl: Time to live for the message. If > 1 - this is a multi-hop message
        """
        if ttl > 1:
            # This is a block for gossip
            packet = (
                RawBlockBroadcastPayload(block, ttl)
                if type(block) is bytes
                else BlockBroadcastPayload(*block.block_args(), ttl)
            )
        else:
            packet = (
                RawBlockPayload(block)
                if type(block) is bytes
                else block.to_block_payload()
            )
        for p in peers:
            self.send_packet(p, packet, sig=False)

    @lazy_wrapper_unsigned(RawBlockPayload)
    def received_raw_block(self, peer: Peer, payload: RawBlockPayload) -> None:
        pass

    @lazy_wrapper_unsigned(RawBlockBroadcastPayload)
    def received_raw_block_broadcast(self, peer: Peer, payload: RawBlockBroadcastPayload) -> None:
        pass

    @lazy_wrapper_unsigned(BlockPayload)
    def received_block(self, peer: Peer, payload: BlockPayload):
        block = PlexusBlock.from_payload(payload, self.serializer)
        self.process_block(block, peer)
        self.incoming_block_queue.put_nowait((peer, block))

    @synchronized
    def sign_block(
            self,
            counterparty_peer=None,
            block_type=b"unknown",
            transaction=None,
            com_id=None,
            links=None,
            fork_seq=None,
    ):
        if not transaction:
            transaction = b""
        block = PlexusBlock.create(
            block_type,
            transaction,
            self.persistence,
            self.my_peer.public_key.key_to_bin(),
            com_id,
            links,
            fork_seq,
        )
        self.logger.info("Signing the block %s", block)
        block.sign(self.my_peer.key)
        if not self.persistence.contains(block):
            self.persistence.add_block(block)
            self.notify_listeners(block)

        # Is there a counter-party we need to send the block first?
        if counterparty_peer == self.my_peer or not counterparty_peer:
            # We created a self-signed block / initial claim, send to the neighbours
            if (
                    block.type not in self.settings.block_types_bc_disabled
                    and not self.settings.is_hiding
            ):
                self.send_block(block)
            return succeed(block)
        else:
            # There is a counter-party to sign => Send to the counter-party first
            self.send_block(block, address=counterparty_peer.address)
            # TODO: send to the community?
            self.send_block(block)
            return succeed(block)

    def self_sign_block(
            self,
            block_type=b"unknown",
            transaction=None,
            com_id=None,
            links=None,
            fork_seq=None,
    ):
        return self.sign_block(
            self.my_peer,
            block_type=block_type,
            transaction=transaction,
            com_id=com_id,
            links=links,
            fork_seq=fork_seq,
        )

    async def evaluate_incoming_block_queue(self):
        while True:
            block_info = await self.incoming_block_queue.get()
            peer, block = block_info

            await self.process_block(block, peer)
            await sleep(self.settings.block_queue_interval / 1000)

    @synchronized
    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, BlockBroadcastPayload)
    def received_block_broadcast(self, source_address, dist, payload):
        """
        We received a half block, part of a broadcast. Disseminate it further.
        """
        block = self.get_block_class(payload.type).from_payload(
            payload, self.serializer
        )
        peer = Peer(payload.public_key, source_address)
        self.validate_persist_block(block, peer)

        if block.hash not in self.relayed_broadcasts and payload.ttl > 1:
            self.send_block(block, ttl=payload.ttl)

    def validate_persist_block(self, block: Union[PlexusBlock, bytes], peer: Peer = None):
        """
        Validate a block and if it's valid, persist it. Return the validation result.
        :param block: The block to validate and persist.
        :return: [ValidationResult]
        """
        block_blob = block if type(block) is bytes else block.pack()
        block = PlexusBlock.unpack(block_blob, self.serializer)
        if not block.block_invariants_valid():
            # React on invalid block
            self.logger.warning("Received invalid block! %s", block)
        else:
            if not self.persistence.has_block(block.hash):
                self.persistence.add_block(block_blob, block)

    @synchronized
    async def process_block(
            self, blk: PlexusBlock, peer, status=None, audit_proofs=None
    ):
        """
        Process a received half block.
        """
        self.validate_persist_block(blk, peer)

        # Notify to validate?
        # TODO add bilateral agreements

    def choose_community_peers(self, com_peers, current_seed, commitee_size):
        rand = random.Random(current_seed)
        return rand.sample(com_peers, commitee_size)

    # ------ State-based synchronization -------------
    def request_state(
            self, peer_address, chain_id, state_name=None, include_other_witnesses=True
    ):
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        self.logger.debug("Requesting state from a peer (%s) ", peer_address)

        state_request = {"state": state_name, "include_others": include_other_witnesses}
        serialized = json.dumps(state_request)
        payload = StateRequestPayload(chain_id, serialized).to_pack_list()
        packet = self._ez_pack(self._prefix, STATE_REQ_MSG, [dist, payload], False)
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, StateRequestPayload)
    def received_state_request(
            self, source_address, dist, payload: StateRequestPayload
    ):
        # I'm part of the community?
        if payload.key in self.my_subscriptions:
            # chain is known
            # TODO: handle bad format
            deserial = json.loads(payload.value)
            state_name = deserial.get("state")
            include_other_witnesses = deserial.get("include_others")

            max_votes = self.persistence.get_latest_max_state_votes(
                payload.key, state_name
            )
            # Analyze max votes state
            d = defaultdict(list)
            if max_votes:
                seq_num, votes = max_votes
                my_id = hexlify(self.my_peer.public_key.key_to_bin()).decode()
                if not any(my_id in i for i in votes):
                    # My peer didn't sign it => add own vote
                    state = self.persistence.get_state(payload.key, seq_num)
                    my_signed_state = self.sign_state(state)
                    my_signed_state[my_signed_state[2]].add(
                        (my_signed_state[0], my_signed_state[1])
                    )
                    votes.add(my_signed_state)
                    self.persistence.add_state_vote(
                        payload.key, seq_num, my_signed_state
                    )
                if include_other_witnesses:
                    for p_id, sign, state_hash in votes:
                        d[state_hash].append((p_id, sign))
                d = dict(d)
                self.send_state_response(source_address, payload.key, json.dumps(d))
        else:
            # TODO: add reject
            pass

    def send_state_response(self, peer_address, chain_id, state_votes):
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        self.logger.debug("Sending state to a peer (%s) ", peer_address)
        payload = StateResponsePayload(chain_id, state_votes).to_pack_list()
        packet = self._ez_pack(self._prefix, STATE_RESP_MSG, [dist, payload], False)
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, StateResponsePayload)
    def received_state_response(
            self, source_address, dist, payload: StateResponsePayload
    ):
        chain_id = payload.key
        hash_val = self.verify_state(json.loads(payload.value))
        if not hash_val:
            self.logger.error("The state is not valid!!")
        else:
            # If state is not know => request it
            if not self.persistence.get_state_by_hash(chain_id, hash_val):
                # TODO: add cache here
                self.send_state_by_hash_request(source_address, chain_id, hash_val)

    def send_state_by_hash_request(self, peer_address, chain_id, state_hash):
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()
        self.logger.debug("Requesting state by hash from peer (%s) ", peer_address)
        payload = StateByHashRequestPayload(chain_id, state_hash).to_pack_list()
        packet = self._ez_pack(
            self._prefix, STATE_BY_HASH_REQ_MSG, [dist, payload], False
        )
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, StateByHashRequestPayload)
    def received_state_by_hash_request(
            self, source_address, dist, payload: StateByHashRequestPayload
    ):
        chain_id = payload.key
        hash_val = payload.value
        state = self.persistence.get_state_by_hash(chain_id, hash_val)
        self.send_state_by_hash_response(source_address, chain_id, state)

    def send_state_by_hash_response(self, peer_address, chain_id, state):
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()
        payload = StateByHashResponsePayload(chain_id, json.dumps(state)).to_pack_list()
        packet = self._ez_pack(
            self._prefix, STATE_BY_HASH_RESP_MSG, [dist, payload], False
        )
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, StateByHashResponsePayload)
    def received_state_by_hash_response(
            self, source_address, dist, payload: StateByHashResponsePayload
    ):
        chain_id = payload.key
        state, seq_num = json.loads(payload.value)
        self.persistence.dump_state(chain_id, seq_num, state)

    async def state_sync(self, community_id, state_name=None):
        """
        Synchronise latest accumulated state in a community.
        Note that it might not work for all use cases
        """
        state = self.persistence.get_latest_state(community_id, state_name)
        # Get the peer list for the community
        peer_list = self.pex[community_id].get_peers()

    def trigger_security_alert(self, peer_id, errors, com_id=None):
        tx = {"errors": errors, "peer": peer_id}
        # TODO attach proof to transaction
        self.self_sign_block(block_type=b"alert", transaction=tx, com_id=com_id)

    def validate_audit_proofs(self, raw_status, raw_audit_proofs, block):
        # TODO: implement
        return True

    def finalize_audits(self, audit_seq, status, audits):
        # TODO: implement
        pass

    # ----------- Auditing chain state wrp invariants ----------------

    def get_all_communities_peers(self):
        peers = set()
        for com_id in self.my_subscriptions:
            vals = self.peer_subscriptions.get(com_id)
            if vals:
                peers.update(vals)
        return peers

    async def unload(self):
        self.logger.debug("Unloading the Plexus Community.")
        self.shutting_down = True

        await self.request_cache.shutdown()

        if self.mem_db_flush_lc:
            self.mem_db_flush_lc.cancel()
        for mid in self.my_subscriptions:
            if mid in self.periodic_sync_lc and not self.periodic_sync_lc[mid].done():
                self.periodic_sync_lc[mid].cancel()

        # Stop queues
        if not self.incoming_block_queue_task.done():
            self.incoming_block_queue_task.cancel()
        if not self.outgoing_block_queue_task.done():
            self.outgoing_block_queue_task.cancel()
        # if not self.audit_response_queue_task.done():
        #    self.audit_response_queue_task.cancel()

        await super(PlexusCommunity, self).unload()

        # Close the persistence layer
        self.persistence.close()


class PlexusTestnetCommunity(PlexusCommunity):
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
