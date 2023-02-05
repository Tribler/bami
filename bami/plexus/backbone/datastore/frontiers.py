from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from bami.plexus.backbone.utils import (
    decode_raw,
    Dot,
    encode_raw,
    expand_ranges,
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
            {b"t": self.terminal, b"h": self.holes, b"i": self.inconsistencies}
        )

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        front_dict = decode_raw(bytes_frontier)
        return cls(front_dict.get(b"t"), front_dict.get(b"h"), front_dict.get(b"i"),)

    def __gt__(self, other: Frontier) -> bool:
        """Frontier is older if one of these holds:
          - max terminal is bigger
          - number of holes is less
          - holds less inconsistencies
          - has more terminal nodes
          """
        newer = max(self.terminal)[0] > max(other.terminal)[0]

        not_more_holes = len(expand_ranges(self.holes)) <= len(
            expand_ranges(other.holes)
        )
        less_holes = len(expand_ranges(self.holes)) < len(expand_ranges(other.holes))
        less_inconsistent = len(self.inconsistencies) < len(other.inconsistencies)
        not_more_inconsistent = len(self.inconsistencies) <= len(other.inconsistencies)
        more_details_known = len(self.terminal) > len(other.terminal)

        return (
            newer
            or less_holes
            or (not_more_holes and less_inconsistent)
            or (not_more_holes and not_more_inconsistent and more_details_known)
        )


@dataclass
class FrontierDiff:
    missing: Ranges
    conflicts: Dict[Dot, Dict[int, Tuple[ShortKey]]]

    def to_bytes(self) -> bytes:
        return encode_raw({b"m": self.missing, b"c": self.conflicts})

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        val_dict = decode_raw(bytes_frontier)
        return cls(val_dict.get(b"m"), val_dict.get(b"c"))

    def is_empty(self):
        return len(self.missing) == 0 and len(self.conflicts) == 0
