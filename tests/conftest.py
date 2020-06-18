# tests/conftest.py
from typing import Any, List, Union
from unittest.mock import Mock

import pytest
from _pytest.config import Config
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.private.libnaclkey import LibNaCLSK
from pytest_mock import MockFixture
from python_project.backbone.block import PlexusBlock, EMPTY_SIG
from python_project.backbone.datastore.chain_store import BaseChain
from python_project.backbone.datastore.state_store import BaseStateStore
from python_project.backbone.datastore.utils import (
    Links,
    ShortKey,
    encode_links,
    encode_raw,
    wrap_return,
)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")


@pytest.fixture
def mock_requests_get(mocker: MockFixture) -> Mock:
    mock = mocker.patch("requests.get")
    mock.return_value.__enter__.return_value.json.return_value = {
        "title": "Lorem Ipsum",
        "extract": "Lorem ipsum dolor sit amet",
    }
    return mock


GENESIS_LINK = Links(((0, ShortKey("30303030")),))


@pytest.fixture
def genesis_link():
    return GENESIS_LINK


# Fixtures
class TestBlock(PlexusBlock):
    """
    Test Block that simulates a block used in TrustChain.
    Also used in other test files for TrustChain.
    """

    def __init__(
        self,
        transaction: bytes = encode_raw({"id": 42}),
        previous: Links = None,
        key: LibNaCLSK = None,
        links: Links = None,
        com_id: Any = None,
        block_type: bytes = b"test",
    ):
        crypto = default_eccrypto
        if not links:
            links = GENESIS_LINK
            com_seq_num = 1
        else:
            com_seq_num = max(links)[0] + 1

        if not previous:
            previous = GENESIS_LINK
        pers_seq_num = max(previous)[0] + 1

        if not com_id:
            com_id = crypto.generate_key(u"curve25519").pub().key_to_bin()

        if key:
            self.key = key
        else:
            self.key = crypto.generate_key(u"curve25519")

        PlexusBlock.__init__(
            self,
            (
                block_type,
                transaction,
                self.key.pub().key_to_bin(),
                pers_seq_num,
                encode_links(previous),
                encode_links(links),
                com_id,
                com_seq_num,
                EMPTY_SIG,
                0,
                0,
            ),
        )
        self.sign(self.key)


def create_block_batch(com_id, num_blocks=100):
    blocks = []
    last_block_point = GENESIS_LINK
    for k in range(num_blocks):
        blk = TestBlock(com_id=com_id, links=last_block_point)
        blocks.append(blk)
        last_block_point = Links(((blk.com_seq_num, blk.short_hash),))
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100):
        key = default_eccrypto.generate_key(u"curve25519")
        com_id = key.pub().key_to_bin()
        return [create_block_batch(com_id, num_blocks) for _ in range(num_batches)]

    return _create_batches


def insert_batch_seq(
    chain_obj: Union[BaseChain, BaseStateStore], batch: List[PlexusBlock]
) -> None:
    for blk in batch:
        yield chain_obj.add_block(blk)


def insert_batch_reversed(
    chain_obj: Union[BaseChain, BaseStateStore], batch: List[PlexusBlock]
) -> None:
    for blk in reversed(batch):
        yield chain_obj.add_block(blk)


def insert_batch_random(
    chain_obj: Union[BaseChain, BaseStateStore], batch: List[PlexusBlock]
) -> None:
    from random import shuffle

    shuffle(batch)
    for blk in batch:
        yield chain_obj.add_block(blk)


batch_insert_functions = [insert_batch_seq, insert_batch_random, insert_batch_reversed]


@pytest.fixture(params=batch_insert_functions)
def insert_function(request):
    param = request.param
    return param


insert_function_copy = insert_function
