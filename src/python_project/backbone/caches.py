import logging
import time
from asyncio import Future
from binascii import hexlify

from ipv8.requestcache import NumberCache, RandomNumberCache
from python_project.backbone.block import PlexusBlock
from python_project.backbone.community import BlockResponse
from python_project.backbone.datastore.utils import hex_to_int


class BlockSignCache(NumberCache):
    CACHE_PREFIX = u"block_validate"

    def __init__(
        self, community, cache_id: int, block_time_delta: float = None
    ) -> None:
        """

        Args:
            community:
            number:
            block_time_delta:
        """
        if not block_time_delta:
            block_time_delta = community.settings.half_block_timeout
        self._delta = block_time_delta
        self.community = community
        self.blocks = {}
        self.cache_id = cache_id

        super().__init__(
            community.request_cache, BlockSignCache.CACHE_PREFIX, self.cache_id
        )

    @property
    def timeout_delay(self) -> float:
        # Timeout for the block verification
        return self._delta

    def add_block(self, block: PlexusBlock) -> None:
        self.blocks[block] = 0

    def confirm_block(self, b: PlexusBlock) -> None:
        self.community.confirm(b)

    def reject_block(self, b: PlexusBlock) -> None:
        self.community.reject(b)

    def process_blocks(self):
        for b, wait_time in list(self.blocks.items()):
            res = self.community.block_response(b, wait_time, 0)
            if res == BlockResponse.CONFIRM:
                self.confirm_block(b)
                # pop block from the blocks
                self.blocks.pop(b)
            elif res == BlockResponse.REJECT:
                self.reject_block(b)
                self.blocks.pop(b)
            else:
                self.blocks[b] += self._delta

    def on_timeout(self) -> None:
        # Verify the chain on update on the risk of invariant violations
        self.process_blocks()

        # Delay further
        if len(self.blocks) > 0:

            async def add_later():
                self.community.request_cache.add(
                    BlockSignCache(self.community, self.cache_id, self._delta)
                )

            self.community.request_cache.register_anonymous_task(
                "add-later", add_later, delay=0.00
            )


class WitnessBlockCache(NumberCache):
    CACHE_PREFIX = u"witness-create"

    def __init__(
        self, community, chain_id: bytes, seq_num: int, delta_time: float = None
    ) -> None:
        """

        Args:
            community:
            number:
            block_time_delta:
        """
        if not delta_time:
            delta_time = community.settings.witness_delta_time
        self._delta = delta_time
        self.community = community
        self.chain_id = chain_id
        self.seq_num = seq_num
        self.cache_id = hex_to_int(chain_id + bytes(seq_num))
        self.proceed = True

        super().__init__(
            community.request_cache, WitnessBlockCache.CACHE_PREFIX, self.cache_id
        )

    @property
    def timeout_delay(self) -> float:
        # Timeout for the block verification
        return self._delta

    def reschedule(self):
        self.proceed = False

    def witness_chain(self):
        self.community.witness(self.chain_id, self.seq_num)

    def on_timeout(self) -> None:
        # Verify the chain on update on the risk of invariant violations
        if self.proceed:
            self.witness_chain()
        else:
            # Delay further
            async def add_later():
                self.community.request_cache.add(
                    WitnessBlockCache(
                        self.community, self.cache_id, self.seq_num, self._delta
                    )
                )

            self.community.request_cache.register_anonymous_task(
                "add-later", add_later, delay=0.00
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
