class BamiSettings(object):
    """
    This class holds various settings regarding TrustChain.
    """

    def __init__(self):
        # Push gossip properties: fanout and ttl (number of hops)
        self.push_gossip_fanout = 9
        self.push_gossip_ttl = 1

        # Track chains of every overlay neighbour
        self.track_neighbours_chains = False

        # Whether frontier gossip is enabled
        self.frontier_gossip_enabled = True
        # Maximum delay before starting the frontier sync in the community
        self.frontier_gossip_sync_max_delay = 0.1
        # The interval at which we gossip the latest frontier in each community
        self.frontier_gossip_interval = 0.5
        # The waiting time between processing two collected frontiers
        self.frontier_gossip_collect_time = 0.2
        # Gossip fanout for frontiers exchange
        self.frontier_gossip_fanout = 6

        self.block_sign_delta = 0.3

        # working directory for the database
        self.work_directory = ".block_db"

        # The maximum and minimum number of peers in the main communities
        self.main_min_peers = 20
        self.main_max_peers = 30

        # The maximum and minimum number of peers in sub-communities
        self.subcom_min_peers = 20
        self.subcom_max_peers = 30
