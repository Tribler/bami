# tests/conftest.py
import collections
from typing import Any, List, Union

import pytest
from _pytest.config import Config

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.private.libnaclkey import LibNaCLSK

from bami.backbone.block import EMPTY_SIG, BamiBlock
from bami.backbone.datastore.chain_store import BaseChain, Chain
from bami.backbone.datastore.database import BaseDB
from bami.backbone.utils import (
    encode_links,
    encode_raw,
    GENESIS_LINK,
    Links,
)
from tests.mocking.base import (
    create_and_connect_nodes,
    SetupValues,
    unload_nodes,
)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")


# Fixtures
class FakeBlock(BamiBlock):
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
        com_prefix: bytes = b"",
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
            com_id = crypto.generate_key("curve25519").pub().key_to_bin()

        if key:
            self.key = key
        else:
            self.key = crypto.generate_key("curve25519")

        if not transaction:
            transaction = encode_raw({"id": 42})

        BamiBlock.__init__(
            self,
            (
                block_type,
                transaction,
                self.key.pub().key_to_bin(),
                pers_seq_num,
                encode_links(previous),
                encode_links(links),
                com_prefix,
                com_id,
                com_seq_num,
                EMPTY_SIG,
                0,
                0,
            ),
        )
        self.sign(self.key)


def create_block_batch(com_id, num_blocks=100):
    """
    Create a given amount of blocks. Each block points to the previous one.

    Args:
        com_id: The community identifier of the block batch.
        num_blocks: The number of blocks to create.

    Returns: A list with blocks.

    """
    blocks = []
    last_block_point = GENESIS_LINK
    for k in range(num_blocks):
        blk = FakeBlock(com_id=com_id, links=last_block_point, transaction=None)
        blocks.append(blk)
        last_block_point = Links(((blk.com_seq_num, blk.short_hash),))
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100):
        """
        Creates batches of blocks within a random community.

        Args:
            num_batches: The number of batches to consider.
            num_blocks: The number of blocks in each batch.

        Returns: A list of batches where each batch represents a chain of blocks.

        """
        key = default_eccrypto.generate_key("curve25519")
        com_id = key.pub().key_to_bin()
        return [create_block_batch(com_id, num_blocks) for _ in range(num_batches)]

    return _create_batches


def insert_to_chain(chain_obj: BaseChain, blk: BamiBlock, personal_chain: bool = True):
    block_links = blk.links if not personal_chain else blk.previous
    block_seq_num = blk.com_seq_num if not personal_chain else blk.sequence_number
    yield chain_obj.add_block(block_links, block_seq_num, blk.hash)


def insert_to_chain_or_blk_store(
    chain_obj: Union[BaseChain, BaseDB], blk: BamiBlock, personal_chain: bool = True,
):
    if isinstance(chain_obj, BaseChain):
        yield from insert_to_chain(chain_obj, blk, personal_chain)
    else:
        yield chain_obj.add_block(blk.pack(), blk)


def insert_batch_seq(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[BamiBlock],
    personal_chain: bool = False,
) -> None:
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_reversed(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[BamiBlock],
    personal_chain: bool = False,
) -> None:
    for blk in reversed(batch):
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_random(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[BamiBlock],
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


@pytest.fixture
def chain():
    return Chain()


insert_function_copy = insert_function

_DirsNodes = collections.namedtuple("DirNodes", ("dirs", "nodes"))


def _set_vals_init(tmpdir_factory, overlay_class, num_nodes) -> _DirsNodes:
    dirs = [
        tmpdir_factory.mktemp(str(overlay_class.__name__), numbered=True)
        for _ in range(num_nodes)
    ]
    nodes = create_and_connect_nodes(num_nodes, work_dirs=dirs, ov_class=overlay_class)

    return _DirsNodes(dirs, nodes)


def _set_vals_teardown(dirs) -> None:
    for k in dirs:
        k.remove(ignore_errors=True)


def _init_nodes(nodes, community_id) -> None:
    for node in nodes:
        node.overlay.subscribe_to_subcom(community_id)


@pytest.fixture
async def set_vals_by_key(
    tmpdir_factory, overlay_class, num_nodes: int, init_nodes: bool
):
    dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    community_key = default_eccrypto.generate_key("curve25519").pub()
    community_id = community_key.key_to_bin()
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    _set_vals_teardown(dirs)


@pytest.fixture
async def set_vals_by_nodes(
    tmpdir_factory, overlay_class, num_nodes: int, init_nodes: bool
):
    dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    community_id = nodes[0].overlay.my_pub_key_bin
    context = nodes[0].overlay.state_db.context
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id, context=context)
    await unload_nodes(nodes)
    _set_vals_teardown(dirs)
