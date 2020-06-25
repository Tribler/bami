from dataclasses import dataclass

from python_project.backbone.datastore.utils import (
    Links,
    Ranges,
    encode_raw,
    decode_raw,
)


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
        return cls(front_dict.get("t"), front_dict.get("h"), front_dict.get("i"))


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
