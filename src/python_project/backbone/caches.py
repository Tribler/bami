import logging
import time
from asyncio import Future
from binascii import hexlify
from functools import reduce

from python_project.backbone.consts import COMMUNITY_CACHE
from python_project.backbone.datastore.utils import (
    expand_ranges,
    hex_to_int,
)
from ipv8.requestcache import NumberCache, RandomNumberCache


class IntroCrawlTimeout(NumberCache):
    """
    A crawl request is sent with every introduction response. This can happen quite a lot of times per second.
    We wish to slow down the amount of crawls we do to not overload any node with database IO.
    """

    def __init__(self, community, peer, identifier=u"introcrawltimeout"):
        super(IntroCrawlTimeout, self).__init__(
            community.request_cache, identifier, self.get_number_for(peer)
        )

    @classmethod
    def get_number_for(cls, peer):
        """
        Convert a Peer into an int. To do this we shift every byte of the mid into an integer.
        """
        charlist = []
        for i in range(len(peer.mid)):
            charlist.append(ord(peer.mid[i : i + 1]))
        return reduce(lambda a, b: ((a << 8) | b), charlist, 0)

    @property
    def timeout_delay(self):
        """
        We crawl the same peer, at most once every 60 seconds.
        :return:
        """
        return 60.0

    def on_timeout(self):
        """
        This is expected, the super class will now remove itself from the request cache.
        The node is then allowed to be crawled again.
        """
        pass


class BlockSignCache(NumberCache):
    """
    This request cache keeps track of outstanding half block signature requests.
    """

    def __init__(
        self,
        community,
        block,
        sign_future,
        socket_address,
        timeouts=0,
        from_peer=None,
        seq_num=None,
    ):
        """
        A cache to keep track of the signing of one of our blocks by a counterparty.

        :param community: the PlexusCommunity
        :param half_block: the half_block requiring a counterparty
        :param sign_future: the Deferred to fire once this block has been double signed
        :param socket_address: the peer we sent the block to
        :param timeouts: the number of timeouts we have already had while waiting
        """
        block_id_int = int(hexlify(block.block_id), 16) % 100000000
        super(BlockSignCache, self).__init__(
            community.request_cache, u"sign", block_id_int
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.community = community
        self.half_block = block
        self.sign_future = sign_future
        self.socket_address = socket_address
        self.timeouts = timeouts
        self.from_peer = from_peer
        self.seq_num = seq_num

    @property
    def timeout_delay(self):
        """
        Note that we use a very high timeout for a half block signature. Ideally, we would like to have a request
        cache without any timeouts and just keep track of outstanding signature requests but this isn't possible (yet).
        """
        return self.community.settings.half_block_timeout

    def on_timeout(self):
        if self.sign_future.done():
            self._logger.debug(
                "Race condition encountered with timeout/removal of HalfBlockSignCache, recovering."
            )
            return
        self._logger.info(
            "Timeout for sign request for half block %s, note that it can still arrive!",
            self.half_block,
        )
        if self.timeouts < self.community.settings.half_block_timeout_retries:
            self.community.send_block(self.half_block, address=self.socket_address)

            async def add_later():
                self.community.request_cache.add(
                    BlockSignCache(
                        self.community,
                        self.half_block,
                        self.sign_future,
                        self.socket_address,
                        self.timeouts + 1,
                    )
                )

            self.community.request_cache.register_anonymous_task(
                "add-later", add_later, delay=0.0
            )
        else:
            self.sign_future.set_exception(RuntimeError("Signature request timeout"))


class CommunitySyncCache(NumberCache):
    """
    This cache tracks outstanding sync requests with other peers in a community
    """

    def __init__(self, community, chain_id):
        cache_num = hex_to_int(chain_id)
        NumberCache.__init__(self, community.request_cache, COMMUNITY_CACHE, cache_num)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.community = community

        self.chain_id = chain_id
        self.working_front = dict()

    @property
    def timeout_delay(self):
        return self.community.settings.sync_timeout

    def receive_frontier(self, peer_address, frontier):
        self.working_front[peer_address] = frontier

    def process_working_front(self):
        candidate = None
        cand_max = 0
        for peer, front in self.working_front.items():
            to_request, _ = self.community.persistence.reconcile_or_create(
                self.chain_id, front
            )
            if (
                any(to_request.values())
                and len(expand_ranges(to_request["m"])) + len(to_request["c"])
                > cand_max
            ):
                candidate = (peer, to_request)
                cand_max = len(expand_ranges(to_request["m"])) + len(to_request["c"])
        return candidate

    def on_timeout(self):
        # TODO convert this to a queue
        async def add_later():
            try:
                self.community.request_cache.add(
                    CommunitySyncCache(self.community, self.chain_id)
                )
            except RuntimeError:
                pass

        # Process all frontiers received
        cand = self.process_working_front()
        if cand:
            # Send request to candidate peer
            self.community.send_blocks_request(cand[0], self.chain_id, cand[1])
            self.community.request_cache.register_anonymous_task(
                "add-later", add_later, delay=0.0
            )
            if self.community.request_cache.get(
                COMMUNITY_CACHE, hex_to_int(self.chain_id)
            ):
                self.community.request_cache.pop(
                    COMMUNITY_CACHE, hex_to_int(self.chain_id)
                )


class PingRequestCache(RandomNumberCache):
    """
    This request cache keeps track of all outstanding requests within the DHTCommunity.
    """

    def __init__(self, community, msg_type, peer):
        super(PingRequestCache, self).__init__(community.request_cache, msg_type)
        self.community = community
        self.msg_type = msg_type
        self.peer = peer
        self.future = Future()
        self.start_time = time.time()

    @property
    def timeout_delay(self):
        return self.community.settings.ping_timeout

    def on_timeout(self):
        if not self.future.done():
            self._logger.debug("Ping timeout for peer %s", self.peer)
            self.future.set_exception(
                RuntimeError("Ping timeout for peer {}".format(self.peer))
            )
