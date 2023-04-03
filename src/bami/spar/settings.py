import dataclasses


@dataclasses.dataclass
class SPARSettings:
    crawl_interval: int = 0.5
