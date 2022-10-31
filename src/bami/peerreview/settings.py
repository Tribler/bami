class PeerReviewSettings:
    def __init__(self):
        # Transaction creation parameters
        self.script_size = 100  # in bytes
        self.tx_freq = 5  # in seconds
        self.tx_delay = 0

        # Reconciliation settings
        self.recon_freq = 2  # in seconds
        self.recon_fanout = 5  # selected peers for reconciliation
        self.recon_delay = 0

        # Gossip settings
        self.fanout = 10

        self.start_immediately = False
