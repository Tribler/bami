import dataclasses
from enum import Enum


class SketchAlgorithm(Enum):
    MINISKETCH = 1
    BLOOM = 2


class SettlementStrategy(Enum):
    FAIR = 1
    VANILLA = 2
    LOCAL_ORDER = 3


@dataclasses.dataclass
class ClientSettings:
    # Transaction creation parameters
    script_size = 100  # in bytes
    tx_freq = 0.1
    tx_batch = 1  # 1 transaction in
    tx_delay = 0  # initial delay before starting

    initial_fanout = 8


@dataclasses.dataclass
class ReconciliationSettings:
    # Reconciliation settings
    recon_freq = 1  # Reconciliation round frequency
    recon_fanout = 5  # selected peers for reconciliation
    recon_delay = 1  # start first round after the delay

    max_pending_requests = 1


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

    # Default sketch for reconciliation is Bloom Filter
    tx_id_size = 32  # in bits
    tx_batch_size = 6  # in transactions

    # Database settings
    dir_name = 'memcache'

    sketch_algorithm = SketchAlgorithm.MINISKETCH
    sketch_size: int = 100
    max_sections = 500

    # BatchMaker parameters
    batch_size: int = 250  # number of transactions
    batch_freq: float = 1.0  # in seconds
    batch_delay: float = 0.1  # in seconds, delay before starting batch creation

    settle_freq = 5
    settle_delay = 2
    settle_strategy = SettlementStrategy.LOCAL_ORDER
    settle_size = 300

    min_fee = 0.0

    simulate_network_latency: bool = True
    start_immediately: bool = False
