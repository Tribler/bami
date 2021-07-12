from bami.basalt.util import hash


def test_hash():
    hash1 = hash("abc1234", 1234)
    assert len(hash1) == 32

    hash2 = hash("abc1234", 1235)
    assert hash1 != hash2
