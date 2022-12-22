import dataclasses


@dataclasses.dataclass
class ClientSettings:
    # Transaction creation parameters
    script_size = 100  # in bytes
    tx_freq = 5  # 1 transaction in
    tx_delay = 2  # initial delay before starting


@dataclasses.dataclass
class ReconciliationSettings:
    # Reconciliation settings
    recon_freq = 1  # Reconciliation round frequency
    recon_fanout = 7  # selected peers for reconciliation
    recon_delay = 0  # start first round after the delay


@dataclasses.dataclass
class PeerClockSettings:
    n_cells = 32


@dataclasses.dataclass
class LZSettings(ClientSettings,
                 ReconciliationSettings,
                 PeerClockSettings):
    enable_client = True


class PeerReviewSettings:
    def __init__(self):
        # Gossip settings
        self.fanout = 10

        self.start_immediately = False
