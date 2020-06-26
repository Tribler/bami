from dataclasses import dataclass
from typing import Tuple

from python_project.backbone.datastore.utils import (
    Links,
    Ranges,
    encode_raw,
    decode_raw,
    expand_ranges,
)


def convert_to(val):
    return tuple(tuple(t) for t in val)


@dataclass
class Frontier:
    terminal: Links
    holes: Ranges
    inconsistencies: Links
    terminal_bits: Tuple[bool]

    def to_bytes(self) -> bytes:
        return encode_raw(
            {
                "t": self.terminal,
                "h": self.holes,
                "i": self.inconsistencies,
                "b": self.terminal_bits,
            }
        )

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        front_dict = decode_raw(bytes_frontier)
        return cls(
            convert_to(front_dict.get("t")),
            convert_to(front_dict.get("h")),
            convert_to(front_dict.get("i")),
            tuple(front_dict.get("b")),
        )

    @property
    def consistent_terminal(self) -> Links:
        const_links = []
        holes_set = expand_ranges(self.holes)

        for i in range(len(self.terminal)):
            if self.terminal_bits[i]:
                const_links.append(self.terminal[i])

        return Links(tuple(const_links))


@dataclass
class FrontierDiff:
    missing: Ranges
    conflicts: Links

    def to_bytes(self) -> bytes:
        return encode_raw({"m": self.missing, "c": self.conflicts})

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        val_dict = decode_raw(bytes_frontier)
        return cls(val_dict.get("m"), val_dict.get("c"))

    def is_empty(self):
        return len(self.missing) > 0 and len(self.conflicts) > 0
