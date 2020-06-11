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
from typing import List

import orjson as json

from python_project.backbone.datastore.memory_database import PlexusMemoryDatabase
from python_project.backbone.block import EMPTY_PK, PlexusBlock
from python_project.backbone.caches import (
    PingRequestCache,
    CommunitySyncCache,
)
from python_project.backbone.consts import *
from python_project.backbone.datastore.utils import (
    decode_frontier,
    encode_frontier,
    hex_to_int,
    take_hash,
)
from python_project.backbone.listener import BlockListener
from python_project.backbone.payload import *
from python_project.backbone.settings import SecurityMode, PlexusSettings

from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.messaging.payload_headers import (
    BinMemberAuthenticationPayload,
    GlobalTimeDistributionPayload,
)
from ipv8.peer import Peer
from ipv8.requestcache import RequestCache
from ipv8.util import maybe_coroutine, succeed


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


class PlexusCommunity(Community):
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
        working_directory = kwargs.pop("working_directory", "")
        self.persistence = kwargs.pop("persistence", None)
        db_name = kwargs.pop("db_name", self.DB_NAME)
        self.settings = kwargs.pop("settings", PlexusSettings())
        self.receive_block_lock = RLock()
        super(PlexusCommunity, self).__init__(*args, **kwargs)
        self.request_cache = RequestCache()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.persistence = PlexusMemoryDatabase(working_directory, db_name)

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
        self.my_subscriptions = list()

        self.peer_subscriptions = (
            dict()
        )  # keeps track of which communities each peer is part of
        self.bootstrap_master = None
        self.proof_requests = {}

        self.decode_map.update(
            {
                chr(BLOCKS_REQ_MSG): self.received_blocks_request,
                chr(BLOCK_MSG): self.received_block,
                chr(BLOCK_CAST_MSG): self.received_block_broadcast,
                chr(FRONTIER_MSG): self.received_frontier,
                chr(SUBS_MSG): self.received_subs_update,
                chr(STATE_REQ_MSG): self.received_state_request,
                chr(STATE_RESP_MSG): self.received_state_response,
                chr(STATE_BY_HASH_REQ_MSG): self.received_state_by_hash_request,
                chr(STATE_BY_HASH_RESP_MSG): self.received_state_by_hash_response,
            }
        )

        # Enable the memory database
        orig_db = self.persistence

    # ----- SubCommunity routines ------
    def is_subscribed(self, community_id: bytes) -> bool:
        return community_id in self.my_subscriptions

    def subscribe_to_multi_community(self, communties: List[bytes]) -> None:
        """
        Subscribe to the community with the public key master peer.
        Community is identified with a peer.mid.

        If bootstrap_master is not specified will use RandomWalks to discover other peers for the same community.
        Peer will be connect to maximum  `settings.max_peers_subtrust` peers.
        """
        for c_id in communties:
            if c_id not in self.my_subscriptions:
                self.my_subscriptions.append(c_id)
                # Join the protocol audits
                self.join_community_gossip(
                    c_id, self.settings.security_mode, self.settings.sync_time
                )

        # Find other peers in the community
        for p in self.get_peers():
            # Send them new subscribe collection
            self.send_subs_update(p.address, self.my_subscriptions)

    def subscribe_to_community(
        self, community_id: bytes, personal: bool = False
    ) -> None:
        """
        Subscribe to the SubCommunity with the public key master peer.
        Community is identified with a community_id.

        If bootstrap_master is not specified will use RandomWalks to discover other peers for the same community.
        Peer will be connect to maximum  `settings.max_peers_subtrust` peers.

        Args:
            community_id: bytes identifier of the community
            personal: this is community is on personal chain
        """
        if community_id not in self.my_subscriptions:
            self.my_subscriptions.append(community_id)
            self.logger.info(
                "Joining community with mid %s (personal? %s)", community_id, personal
            )

            # Find other peers in the community
            for p in self.get_peers():
                # Send them new subscribe collection
                self.send_subs_update(p.address, self.my_subscriptions)

            # Join the protocol audits
            self.join_community_gossip(
                community_id, self.settings.security_mode, self.settings.sync_time
            )

    def send_subs_update(self, peer_address, peer_subs):
        """
        Send to all known peer subscription update
        """
        decoded_list = [hexlify(x).decode() for x in peer_subs]
        global_time = self.claim_global_time()
        auth = BinMemberAuthenticationPayload(
            self.my_peer.public_key.key_to_bin()
        ).to_pack_list()
        payload = SubscriptionsPayload(
            self.my_peer.public_key.key_to_bin(), json.dumps(decoded_list)
        ).to_pack_list()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()
        packet = self._ez_pack(self._prefix, SUBS_MSG, [auth, dist, payload])
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper(GlobalTimeDistributionPayload, SubscriptionsPayload)
    def received_subs_update(self, peer, dist, payload: SubscriptionsPayload):
        peer_subs = json.loads(payload.value)
        self.process_peer_interests(peer, peer_subs)

    def join_community_gossip(self, community_mid: bytes, mode, sync_time):
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
        @param community_mid: master_peer_mid identification for community
        @param mode: security mode to which join the community: see settings.SecurityMode
        @param sync_time: interval in seconds to run the task
        """
        # Start sync task after the discovery
        task = self.gossip_sync_task if mode == SecurityMode.VANILLA else None

        self.periodic_sync_lc[community_mid] = self.register_task(
            "sync_" + str(community_mid),
            task,
            community_mid,
            delay=random.random(),
            interval=sync_time,
        )

    def sign_state(self, state):
        state_hash = take_hash(state)
        signature = default_eccrypto.create_signature(self.my_peer.key, state_hash)
        # Prepare for send
        my_id = hexlify(self.my_peer.public_key.key_to_bin()).decode()
        signature = hexlify(signature).decode()
        state_hash = hexlify(state_hash).decode()

        return my_id, signature, state_hash

    def verify_state(self, state_val):
        # This is a claim of a conditional transaction
        for hash_val, sig_set in state_val.items():
            if all(
                default_eccrypto.is_valid_signature(
                    default_eccrypto.key_from_public_bin(unhexlify(p_id)),
                    unhexlify(hash_val),
                    unhexlify(sign),
                )
                for p_id, sign in sig_set
            ):
                return unhexlify(hash_val)
            else:
                return None

    @synchronized
    def gossip_sync_task(self, community_id):
        frontier = self.persistence.get_frontier(community_id)
        self.logger.debug("Gossip sync %s (%s) ", community_id, frontier)
        if frontier and "v" in frontier:
            seq_num = max(frontier["v"])[0]
            # Include the state in the frontier dissemination or not?

            state = (
                self.persistence.get_state(community_id, seq_num)
                if self.persistence.is_state_consistent(community_id)
                else None
            )
            # sign state => sign and send hash
            if state:
                frontier["state"] = self.sign_state(state)
                self.persistence.add_state_vote(
                    community_id, seq_num, frontier["state"]
                )

            # select max num randomly
            peer_set = self.peer_subscriptions[community_id]
            f = min(len(peer_set), self.settings.gossip_fanout)
            self.send_frontier(community_id, frontier, random.sample(peer_set, f))

    def send_frontier(self, community_id, frontier, peers):
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        self.logger.debug("Gossiping frontier (%s)", frontier)

        serialized = json.dumps(decode_frontier(frontier))

        payload = FrontierPayload(community_id, serialized).to_pack_list()
        packet = self._ez_pack(self._prefix, FRONTIER_MSG, [dist, payload], False)

        for p in peers:
            self.endpoint.send(p.address, packet)

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, FrontierPayload)
    def received_frontier(self, source_address, dist, payload: FrontierPayload):
        frontier = encode_frontier(json.loads(payload.value))
        chain_id = payload.key

        cache = self.request_cache.get(COMMUNITY_CACHE, hex_to_int(chain_id))
        if cache:
            cache.receive_frontier(source_address, frontier)
        else:
            # Create new cache
            # TODO: what to do with `send` diff - revisit
            to_request, to_send = self.persistence.reconcile_or_create(
                chain_id, frontier
            )
            if any(to_request.values()):
                self.send_blocks_request(source_address, chain_id, to_request)
                self.request_cache.add(CommunitySyncCache(self, chain_id))

    def send_blocks_request(self, peer_address, chain_id, request_set):
        """
        Request blocks for a peer from a chain
        """
        self._logger.debug(
            "Requesting blocks %s from peer %s:%d",
            request_set,
            peer_address[0],
            peer_address[1],
        )
        global_time = self.claim_global_time()
        auth = BinMemberAuthenticationPayload(
            self.my_peer.public_key.key_to_bin()
        ).to_pack_list()
        payload = BlocksRequestPayload(
            chain_id, json.dumps(decode_frontier(request_set))
        ).to_pack_list()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        packet = self._ez_pack(self._prefix, BLOCKS_REQ_MSG, [auth, dist, payload])
        self.endpoint.send(peer_address, packet)

    @lazy_wrapper(GlobalTimeDistributionPayload, BlocksRequestPayload)
    def received_blocks_request(self, peer, dist, payload: BlocksRequestPayload):
        blocks_request = encode_frontier(json.loads(payload.value))
        self._logger.debug("Received block request %s {%s}", blocks_request, peer)
        chain_id = payload.key
        blocks = self.persistence.get_blocks_by_request(chain_id, blocks_request)
        self.send_multi_blocks(peer.address, chain_id, blocks)

    def send_multi_blocks(self, address, chain_id, blocks):
        self._logger.debug("Sending blocks %s to {%s}", blocks, address)
        for block in blocks:
            global_time = self.claim_global_time()
            dist = GlobalTimeDistributionPayload(global_time).to_pack_list()
            payload = BlockPayload.from_block(block).to_pack_list()
            packet = self._ez_pack(self._prefix, BLOCK_MSG, [dist, payload], False)
            self.endpoint.send(address, packet)

    def get_peer(self, pub_key):
        for peer in self.get_peers():
            if peer.public_key.key_to_bin() == pub_key:
                return peer
        return None

    # -------- Ping Functions -------------
    async def ping(self, peer):
        self.logger.debug("Pinging peer %s", peer)

        cache = self.request_cache.add(PingRequestCache(self, u"ping", peer))

        global_time = self.claim_global_time()
        auth = BinMemberAuthenticationPayload(
            self.my_peer.public_key.key_to_bin()
        ).to_pack_list()
        payload = PingPayload(cache.number).to_pack_list()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        packet = self._ez_pack(self._prefix, 15, [auth, dist, payload])
        self.endpoint.send(peer.address, packet)

        await cache.future

    @lazy_wrapper(GlobalTimeDistributionPayload, PingPayload)
    def on_ping_request(self, peer, dist, payload):
        self.logger.debug("Got ping-request from %s", peer.address)

        global_time = self.claim_global_time()
        auth = BinMemberAuthenticationPayload(
            self.my_peer.public_key.key_to_bin()
        ).to_pack_list()
        payload = PingPayload(payload.identifier).to_pack_list()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        packet = self._ez_pack(self._prefix, 16, [auth, dist, payload])
        self.endpoint.send(peer.address, packet)

    @lazy_wrapper(GlobalTimeDistributionPayload, PingPayload)
    def on_ping_response(self, peer, dist, payload):
        if not self.request_cache.has(u"ping", payload.identifier):
            self.logger.error(
                "Got ping-response with unknown identifier, dropping packet"
            )
            return

        self.logger.debug("Got ping-response from %s", peer.address)
        cache = self.request_cache.pop(u"ping", payload.identifier)
        cache.future.set_result(None)

    def init_mem_db_flush(self, flush_time):
        if not self.mem_db_flush_lc:
            self.mem_db_flush_lc = self.register_task(
                "mem_db_flush", self.mem_db_flush, flush_time
            )

    def add_listener(self, listener, block_types):
        """
        Add a listener for specific block types.
        """
        for block_type in block_types:
            if block_type not in self.listeners_map:
                self.listeners_map[block_type] = []
            self.listeners_map[block_type].append(listener)
            self.persistence.block_types[block_type] = listener.BLOCK_CLASS

    def remove_listener(self, listener, block_types):
        for block_type in block_types:
            if (
                block_type in self.listeners_map
                and listener in self.listeners_map[block_type]
            ):
                self.listeners_map[block_type].remove(listener)
            if block_type in self.persistence.block_types:
                self.persistence.block_types.pop(block_type, None)

    def get_block_class(self, block_type):
        """
        Get the block class for a specific block type.
        """
        if block_type not in self.listeners_map or not self.listeners_map[block_type]:
            return PlexusBlock

        return self.listeners_map[block_type][0].BLOCK_CLASS

    async def should_sign(self, block):
        """
        Return whether we should sign the block in the passed message.
        @param block: the block we want to sign or not.
        """
        if block.type not in self.listeners_map:
            return False  # There are no listeners for this block

        for listener in self.listeners_map[block.type]:
            should_sign = await maybe_coroutine(listener.should_sign, block)
            if should_sign:
                return True

        return False

    def _add_broadcasted_blockid(self, block_id):
        self.relayed_broadcasts.add(block_id)
        self.relayed_broadcasts_order.append(block_id)
        if len(self.relayed_broadcasts) > self.settings.broadcast_history_size:
            to_remove = self.relayed_broadcasts_order.popleft()
            self.relayed_broadcasts.remove(to_remove)

    def send_block(self, block, address=None, ttl=1):
        """
        Send a block to a specific address, or do a broadcast to known peers if no peer is specified.
        """
        if ttl < 1:
            return
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time).to_pack_list()

        if address:
            self.logger.debug(
                "Sending block to (%s:%d) (%s)", address[0], address[1], block
            )
            payload = BlockPayload.from_block(block).to_pack_list()
            packet = self._ez_pack(self._prefix, BLOCK_MSG, [dist, payload], False)
            self.outgoing_block_queue.put_nowait((address, packet))
        else:
            self.logger.debug(
                "Sending block to a set of peers subscribed to a community %s", block
            )
            payload = BlockBroadcastPayload.from_block_gossip(block, ttl).to_pack_list()
            packet = self._ez_pack(self._prefix, BLOCK_CAST_MSG, [dist, payload], False)

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
            self._add_broadcasted_blockid(block.hash)

    async def evaluate_outgoing_block_queue(self):
        while True:
            packet_info = await self.outgoing_block_queue.get()
            address, packet = packet_info
            self.endpoint.send(address, packet)

            await sleep(self.settings.block_queue_interval / 1000)

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

    @lazy_wrapper_unsigned(GlobalTimeDistributionPayload, BlockPayload)
    async def received_block(self, source_address, dist, payload):
        """
        We've received a half block, either because we sent a SIGNED message to some one or we are crawling
        """
        peer = Peer(payload.public_key, source_address)
        block = self.get_block_class(payload.type).from_payload(
            payload, self.serializer
        )

        self._logger.debug("Received block directly %s", block)

        self.incoming_block_queue.put_nowait((peer, block))

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

    def validate_persist_block(self, block, peer=None):
        """
        Validate a block and if it's valid, persist it. Return the validation result.
        :param block: The block to validate and persist.
        :return: [ValidationResult]
        """
        if not self.persistence.contains(block):
            self.persistence.add_block(block)
        # TODO: Verify invariants

    def notify_listeners(self, block):
        """
        Notify listeners of a specific new block.
        """
        # Call the listeners associated to the universal block, if there are any
        for listener in self.listeners_map.get(self.UNIVERSAL_BLOCK_LISTENER, []):
            listener.received_block(block)

        # Avoid proceeding any further if the type of the block coincides with the UNIVERSAL_BLOCK_LISTENER
        if (
            block.type not in self.listeners_map
            or self.shutting_down
            or block.type == self.UNIVERSAL_BLOCK_LISTENER
        ):
            return

        for listener in self.listeners_map[block.type]:
            listener.received_block(block)

    @synchronized
    async def process_block(
        self, blk: PlexusBlock, peer, status=None, audit_proofs=None
    ):
        """
        Process a received half block.
        """
        self.validate_persist_block(blk, peer)
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

    # ---- Introduction handshakes => Exchange your subscriptions ----------------
    def create_introduction_request(self, socket_address, extra_bytes=b""):
        communities = []
        for community_id in self.my_subscriptions:
            communities.append(hexlify(community_id).decode())
        extra_bytes = json.dumps(communities)
        return super(PlexusCommunity, self).create_introduction_request(
            socket_address, extra_bytes
        )

    def create_introduction_response(
        self,
        lan_socket_address,
        socket_address,
        identifier,
        introduction=None,
        extra_bytes=b"",
        prefix=None,
    ):
        communities = []
        for community_id in self.my_subscriptions:
            communities.append(hexlify(community_id).decode())
        extra_bytes = json.dumps(communities)
        return super(PlexusCommunity, self).create_introduction_response(
            lan_socket_address,
            socket_address,
            identifier,
            introduction,
            extra_bytes,
            prefix,
        )

    def process_peer_interests(self, peer, communities):
        for community in communities:
            community_id = unhexlify(community)
            if community_id not in self.peer_subscriptions:
                self.peer_subscriptions[community_id] = set()
            self.peer_subscriptions[community_id].add(peer)

    def introduction_response_callback(self, peer, dist, payload):
        communities = json.loads(payload.extra_bytes)
        self.process_peer_interests(peer, communities)
        if self.settings.track_neighbours_chains:
            self.subscribe_to_community(peer.public_key.key_to_bin(), personal=True)

    def introduction_request_callback(self, peer, dist, payload):
        communities = json.loads(payload.extra_bytes)
        self.process_peer_interests(peer, communities)
        if self.settings.track_neighbours_chains:
            self.subscribe_to_community(peer.public_key.key_to_bin(), personal=True)

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
