from dataclasses import dataclass
from typing import Dict, Tuple

from python_project.backbone.utils import (
    decode_raw,
    Dot,
    encode_raw,
    Links,
    Ranges,
    ShortKey,
)


def convert_to_tuple_list(val):
    return tuple(tuple(t) for t in val)


def convert_to_tuple_dict(val):
    return dict((k, tuple(v)) for k, v in val.items())


@dataclass
class Frontier:
    terminal: Links
    holes: Ranges
    inconsistencies: Links

    def to_bytes(self) -> bytes:
        return encode_raw(
            {"t": self.terminal, "h": self.holes, "i": self.inconsistencies}
        )

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        front_dict = decode_raw(bytes_frontier)
        return cls(front_dict.get("t"), front_dict.get("h"), front_dict.get("i"),)


@dataclass
class FrontierDiff:
    missing: Ranges
    conflicts: Dict[Dot, Dict[int, Tuple[ShortKey]]]

    def to_bytes(self) -> bytes:
        return encode_raw({"m": self.missing, "c": self.conflicts})

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        val_dict = decode_raw(bytes_frontier)
        return cls(val_dict.get("m"), val_dict.get("c"))

    def is_empty(self):
        return len(self.missing) == 0 and len(self.conflicts) == 0
