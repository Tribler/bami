import hashlib


def hash(str_to_hash: str, seed: int) -> bytes:
    m = hashlib.sha256()
    m.update(str_to_hash.encode())
    m.update(b"%d" % seed)
    return m.digest()
