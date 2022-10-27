class PeerReviewSettings:
    def __init__(self):
        # Transaction creation parameters
        self.script_size = 100  # in bytes
        self.tx_freq = 2  # in seconds

        # Reconciliation settings
        self.recon_freq = 1  # in seconds
        self.recon_fanout = 5  # selected peers for reconciliation

        # Gossip settings
        self.fanout = 10
