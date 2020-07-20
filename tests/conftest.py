# tests/conftest.py
from typing import Any, List, Union
from unittest.mock import Mock

from _pytest.config import Config
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.private.libnaclkey import LibNaCLSK
import pytest
from pytest_mock import MockFixture
from python_project.backbone.block import EMPTY_SIG, PlexusBlock
from python_project.backbone.datastore.chain_store import BaseChain
from python_project.backbone.datastore.database import BaseDB
from python_project.backbone.utils import (
    encode_links,
    encode_raw,
    GENESIS_LINK,
    Links,
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


# Fixtures
class FakeBlock(PlexusBlock):
    """
    Test Block that simulates a block used in TrustChain.
    Also used in other test files for TrustChain.
    """

    def __init__(
        self,
        transaction: bytes = None,
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

        if not transaction:
            transaction = encode_raw({"id": 42})

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


def create_block_batch(com_id, num_blocks=100, txs=None):
    blocks = []
    last_block_point = GENESIS_LINK
    for k in range(num_blocks):
        tx = txs[k] if txs else None
        blk = FakeBlock(com_id=com_id, links=last_block_point, transaction=tx)
        blocks.append(blk)
        last_block_point = Links(((blk.com_seq_num, blk.short_hash),))
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100, txs=None):
        key = default_eccrypto.generate_key(u"curve25519")
        com_id = key.pub().key_to_bin()
        return [
            create_block_batch(com_id, num_blocks, txs[i] if txs else None)
            for i in range(num_batches)
        ]

    return _create_batches


def insert_to_chain(
    chain_obj: BaseChain, blk: PlexusBlock, personal_chain: bool = True
):
    block_links = blk.links if not personal_chain else blk.previous
    block_seq_num = blk.com_seq_num if not personal_chain else blk.sequence_number
    yield chain_obj.add_block(block_links, block_seq_num, blk.hash)


def insert_to_chain_or_blk_store(
    chain_obj: Union[BaseChain, BaseDB], blk: PlexusBlock, personal_chain: bool = True,
):
    if isinstance(chain_obj, BaseChain):
        yield from insert_to_chain(chain_obj, blk, personal_chain)
    else:
        yield chain_obj.add_block(blk.pack(), blk)


def insert_batch_seq(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_reversed(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    for blk in reversed(batch):
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_random(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    from random import shuffle

    shuffle(batch)
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


batch_insert_functions = [insert_batch_seq, insert_batch_random, insert_batch_reversed]


@pytest.fixture(params=batch_insert_functions)
def insert_function(request):
    param = request.param
    return param


insert_function_copy = insert_function
