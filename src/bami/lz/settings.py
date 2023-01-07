import dataclasses


@dataclasses.dataclass
class ClientSettings:
    # Transaction creation parameters
    script_size = 100  # in bytes
    tx_freq = 1
    tx_batch = 3  # 1 transaction in
    tx_delay = 0  # initial delay before starting


@dataclasses.dataclass
class ReconciliationSettings:
    # Reconciliation settings
    recon_freq = 5  # Reconciliation round frequency
    recon_fanout = 7  # selected peers for reconciliation
    recon_delay = 0  # start first round after the delay


@dataclasses.dataclass
class PeerClockSettings:
    n_cells = 32


@dataclasses.dataclass
class BloomFilterSettings:
    bloom_size = 8 * 100
    bloom_num_func = 2
    bloom_max_seed = 255


@dataclasses.dataclass
class LZSettings(ClientSettings,
                 ReconciliationSettings,
                 PeerClockSettings,
                 BloomFilterSettings
                 ):
    enable_client = True
    start_immediately = False

    # Default sketch for reconciliation is Bloom Filter
    tx_id_size = 32  # in bits
    tx_batch_size = 250  # in transactions
