from unittest.mock import Mock

import pytest
from python_project.backbone.datastore.utils import (
    ranges,
    expand_ranges,
    shorten,
    KEY_LEN,
    Links,
    decode_links,
    encode_links,
)
from python_project.noodle.block import GENESIS_HASH, EMPTY_SIG, EMPTY_PK


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


def test_encode_decode_links(keys_fixture):
    links = Links(((1, shorten(keys_fixture)), ))
    raw_bytes = encode_links(links)
    assert decode_links(raw_bytes) == links
