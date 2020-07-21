from enum import Enum


class SecurityMode(Enum):
    """
    Implementations of security implementations of Trustchain.
    """

    VANILLA = 1
    AUDIT = 2
    BOTH = 3


class SyncMode(Enum):
    """
    Gossip synchronization modes
    """

    BLOCK_SYNC = 1
    STATE_SYNC = 2
    FULL_SYNC = 3
    PASSIVE = 4


class BamiSettings(object):
    """
    This class holds various settings regarding TrustChain.
    """

    def __init__(self):
        # Push gossip properties: fanout and ttl (number of hops)
        self.push_gossip_fanout = 5
        self.push_gossip_ttl = 1

        # witness every k block on average with probability 1/K
        self.witness_block_delta = 1
        # Wait time before witnessing
        self.witness_delta_time = 0.4

        # Track chains of every overlay neighbour
        self.track_neighbours_chains = False

        # Time for one frontier gossip round
        self.gossip_sync_max_delay = 0.1
        self.gossip_sync_time = 0.3
        self.gossip_collect_time = 0.4
        self.block_sign_delta = 0.3
        # Maximum wait time 100
        # Maximum wait block 100
        self.max_wait_time = 100
        self.max_wait_block = 100

        # working directory for the database
        self.work_directory = ".block_db"
        # Gossip fanout for frontiers exchange
        self.gossip_fanout = 6

        # Whether we are a crawler (and fetching whole chains)
        self.crawler = False

        # Is the node hiding own blocks?
        self.is_hiding = False

        # Crawling identities
        self.crawlers = [
            b"4c69624e61434c504b3a60001170dcec5f4774e3ea8d5d6b89c98e5b18f10adb3e02b27137d965f1e4188d872bf6a30b6516b98fdb9839f2920ccf42a30a723ab07de7011bbbb245b20b",
            b"4c69624e61434c504b3ad87cdb35fb1025904627aa84483a19e5640a1bebb2f6081e87cd635d22bbbe7cc8467a252d9f5343e10e182939225fea982192396837e9fb4d81fb4f26b74af3",
        ]
