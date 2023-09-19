import dataclasses


@dataclasses.dataclass
class MempoolBroadcastSettings:
    # Transaction creation parameters
    script_size: int = 200  # in bytes
    tx_freq: float = 1 / 200  # x/k: k transaction in x second
    tx_delay: float = 0.05  # Maximum delay before starting transaction creation
    initial_fanout: int = 8  # number of peers to send the transaction to

    # BatchMaker parameters
    batch_size: int = 250  # number of transactions
    batch_freq: float = 0.5  # in seconds
    batch_delay: float = 0.6  # in seconds, delay before starting batch creation

    # Quorum parameters
    quorum_threshold: int = 25  # number of nodes required to progress

    # Sync parameters
    sync_timer_delta: float = 1  # in seconds
    sync_retry_time: float = 5  # in seconds
    sync_retry_nodes: int = 3  # number of nodes

    # Header parameters
    header_delay: float = 0.5  # in seconds
    header_freq: float = 0.2  # in seconds

    start_immediately: bool = False
    simulate_network_latency: bool = True
