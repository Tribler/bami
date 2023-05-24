import dataclasses


@dataclasses.dataclass
class SPARSettings:
    crawl_interval: int = 2
    dummy_score_interval: int = 5
    batch_size: int = 10

    min_slots: int = 4
    max_slots: int = 100
    target_gamma: float = 0.8

    max_blobs: int = 100
    sybil_count: int = 5

    peer_shuffle_interval: int = 200
    rank_recal_interval: int = 20
    rank_recal_delay: int = 11


