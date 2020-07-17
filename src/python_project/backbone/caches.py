from ipv8.requestcache import NumberCache

from python_project.backbone.community import PlexusCommunity
from python_project.backbone.datastore.utils import hex_to_int


class WitnessBlockCache(NumberCache):
    CACHE_PREFIX = u"witness-create"

    def __init__(
        self,
        community: "PlexusCommunity",
        chain_id: bytes,
        seq_num: int,
        delta_time: float = None,
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
        print("Chain id", chain_id)
        print(bytes(seq_num))
        self.cache_id = hex_to_int(chain_id + bytes(seq_num))
        self.proceed = True

        super().__init__(
            community.request_cache, WitnessBlockCache.CACHE_PREFIX, self.cache_id
        )

    @property
    def timeout_delay(self) -> float:
        # Timeout for the block verification
        return self._delta

    def reschedule(self) -> None:
        self.proceed = False

    def witness_chain(self) -> None:
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
                        self.community, self.chain_id, self.seq_num, self._delta
                    )
                )

            self.community.request_cache.register_anonymous_task(
                "add-later", add_later, delay=0.00
            )
