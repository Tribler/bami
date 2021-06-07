import pytest

from unittest.mock import Mock
from bami.plexus.backbone.utils import (
    decode_links,
    decode_raw,
    EMPTY_PK,
    EMPTY_SIG,
    encode_links,
    encode_raw,
    expand_ranges,
    GENESIS_HASH,
    KEY_LEN,
    Links,
    ranges,
    shorten,
)


@pytest.fixture(
    params=[
        ({i for i in range(1, 100)}, ((1, 99),)),
        ({1, 5, 6, 7, 8}, ((1, 1), (5, 8),)),
        ({1}, ((1, 1),)),
        (set(), tuple()),
    ],
    ids=["no_holes", "holes", "one_val", "empty"],
)
def ranges_fixture(request) -> Mock:
    param = request.param
    return param


def test_ranges(ranges_fixture: Mock):
    compressed = ranges(ranges_fixture[0])
    assert compressed == ranges_fixture[1]


def test_expand_ranges(ranges_fixture: Mock):
    decompressed = expand_ranges(ranges_fixture[1])
    assert decompressed == ranges_fixture[0]


@pytest.fixture(
    params=[GENESIS_HASH, EMPTY_SIG, EMPTY_PK], ids=["genesis", "empty_sig", "empty_pk"]
)
def keys_fixture(request) -> Mock:
    return request.param


def test_shorten_size(keys_fixture):
    s_k = shorten(keys_fixture)
    assert len(s_k) == KEY_LEN


def test_encode_decode_raw():
    vals = {b"id": 42}
    assert decode_raw(encode_raw(vals))[b"id"] == 42


def test_encode_decode_bytelist():
    vals = {b"1", b"2", b"3", b"4", b"100", b"10", b"21", b"5"}
    assert set(decode_raw(encode_raw(list(vals)))) == vals


def test_encode_decode_links(keys_fixture):
    links = Links(((1, shorten(keys_fixture)),))
    raw_bytes = encode_links(links)
    assert decode_links(raw_bytes) == links


from decimal import Decimal, getcontext


def test_decimal():
    new_con = getcontext()
    new_con.prec = 4
    t = Decimal(2.191, new_con)
    t2 = Decimal(2.11, new_con)

    l = encode_raw({b"value": float(t2)})
    p = decode_raw(l)
    assert p.get(b"value") == float(t2)
