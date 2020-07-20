from decimal import Decimal

from ipv8.test.mocking.endpoint import internet
import pytest
from python_project.backbone.utils import Dot, GENESIS_LINK
from python_project.backbone.exceptions import InvalidTransactionFormatException
from python_project.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
from python_project.backbone.sub_community import (
    IPv8SubCommunityFactory,
    RandomWalkDiscoveryStrategy,
)
from python_project.payment.community import PaymentCommunity
from python_project.payment.exceptions import (
    InsufficientBalanceException,
    InvalidMintRangeException,
    InvalidSpendRangeException,
    InvalidWitnessTransactionException,
    UnboundedMintException,
    UnknownMinterException,
)

from tests.conftest import FakeBlock
from tests.mocking.base import TestBase


class FakePaymentCommunity(
    IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy, PaymentCommunity
):
    pass


class SetupValues:
    def __init__(self, nodes, context, com_id) -> None:
        self.nodes = nodes
        self.context = context
        self.community_id = com_id
        self.num_nodes = len(self.nodes)


# Add tests for all exceptions
# Add tests on validity of transactions


class TestBackBoneCommunity(TestBase):
    def setup_nodes(self, num_nodes: int):
        self.nodes = []
        internet.clear()
        self.setUp()
        self.initialize(FakePaymentCommunity, num_nodes)

        self.context = self.nodes[0].overlay.state_db.context
        self.community_id = self.nodes[0].overlay.my_pub_key_bin
        for node in self.nodes:
            node.overlay.subscribe_to_subcom(self.community_id)
        self.setUp()

    @pytest.fixture()
    async def std_five(self):
        self._tempdirs = []
        self.setup_nodes(num_nodes=5)
        yield SetupValues(self.nodes, self.context, self.community_id)
        await self.tearDown()

    @pytest.mark.asyncio
    async def test_valid_mint(self, std_five):
        vals = std_five
        minter = vals.nodes[0].overlay.my_pub_key_bin
        vals.nodes[0].overlay.mint(value=Decimal(10, vals.context))
        await self.deliver_messages(0.5)
        for i in range(vals.num_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(minter) > 0

    @pytest.mark.asyncio
    async def test_invalid_mint(self, std_five):
        vals = std_five
        minter = vals.nodes[1].overlay.my_pub_key_bin
        with pytest.raises(UnknownMinterException):
            vals.nodes[1].overlay.mint(value=Decimal(10, vals.context))
        await self.deliver_messages()
        for i in range(vals.num_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(minter) == 0

    def test_invalid_mint_tx_bad_format(self, std_five):
        mint_tx = {}
        chain_id = std_five.community_id
        minter = std_five.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidTransactionFormatException):
            std_five.nodes[0].overlay.verify_mint(chain_id, minter, mint_tx)

    def test_invalid_mint_tx_value_out_of_range(self, std_five):
        mint_tx = {"value": 10 ** 7}
        chain_id = std_five.community_id
        minter = std_five.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidMintRangeException):
            std_five.nodes[0].overlay.verify_mint(chain_id, minter, mint_tx)

    def test_mint_value_unbound_value(self, std_five):
        mint_tx = {"value": 10 ** 7 - 1}
        chain_id = std_five.community_id
        minter = std_five.nodes[0].overlay.my_pub_key_bin
        std_five.nodes[0].overlay.state_db.apply_mint(
            chain_id,
            Dot((1, "123123")),
            GENESIS_LINK,
            minter,
            Decimal(mint_tx.get("value"), std_five.context),
            True,
        )
        next_mint = {"value": 1}
        with pytest.raises(UnboundedMintException):
            std_five.nodes[0].overlay.verify_mint(chain_id, minter, next_mint)

    @pytest.mark.asyncio
    async def test_invalid_spend(self, std_five):
        vals = std_five
        spender = vals.nodes[1].overlay.my_pub_key_bin
        with pytest.raises(InsufficientBalanceException):
            vals.nodes[1].overlay.spend(
                chain_id=vals.community_id,
                counter_party=vals.nodes[1],
                value=Decimal(10, vals.context),
            )

        await self.deliver_messages()
        # Should throw invalid mint exception
        for i in range(vals.num_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(spender) == 0

    def test_invalid_spend_bad_format(self, std_five):
        spend_tx = {}
        minter = std_five.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidTransactionFormatException):
            std_five.nodes[0].overlay.verify_spend(minter, spend_tx)

    def test_invalid_spend_value_out_of_range(self, std_five):
        spend_tx = {
            "value": 10 ** 7,
            "to_peer": std_five.nodes[1].overlay.my_pub_key_bin,
            "prev_pairwise_link": GENESIS_LINK,
        }
        spender = std_five.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidSpendRangeException):
            std_five.nodes[0].overlay.verify_spend(spender, spend_tx)

    @pytest.mark.asyncio
    async def test_valid_spend(self, std_five):
        vals = std_five
        minter = vals.nodes[0].overlay.my_pub_key_bin
        vals.nodes[0].overlay.mint(value=Decimal(10, vals.context))
        spender = minter
        counter_party = vals.nodes[1].overlay.my_pub_key_bin
        vals.nodes[0].overlay.spend(
            chain_id=vals.community_id,
            counter_party=counter_party,
            value=Decimal(10, vals.context),
        )

        assert vals.nodes[0].overlay.state_db.get_balance(spender) == 0

        await self.deliver_messages(0.5)
        # Should throw invalid mint exception
        for i in range(vals.num_nodes):
            assert (
                vals.nodes[i].overlay.state_db.get_balance(spender) == 0
            ), "Peer number {}".format(i)
            assert (
                vals.nodes[i].overlay.state_db.get_balance(counter_party) == 10
            ), "Peer number {}".format(i)
            assert not vals.nodes[i].overlay.state_db.was_balance_negative(spender)

    @pytest.mark.asyncio
    async def test_invalid_spend_ignore_validation(self, std_five):
        vals = std_five
        spender = vals.nodes[1].overlay.my_pub_key_bin
        vals.nodes[1].overlay.spend(
            chain_id=vals.community_id,
            counter_party=vals.nodes[0].overlay.my_pub_key_bin,
            value=Decimal(10, vals.context),
            ignore_validation=True,
        )
        assert vals.nodes[1].overlay.state_db.get_balance(spender) == -10

        await self.deliver_messages(0.5)
        # As the counterparty will reject the block => The spend transaction is reverted
        for i in range(vals.num_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(spender) == 0
            assert vals.nodes[i].overlay.state_db.was_balance_negative(spender)

    # Test witness transaction
    def test_apply_invalid_witness_tx(self, std_five):
        blk = FakeBlock(com_id=std_five.community_id)
        i = 1
        std_five.nodes[0].overlay.witness_delta = 100
        while std_five.nodes[0].overlay.should_witness_chain_point(
            blk.com_id, blk.public_key, i
        ):
            i += 1
        tx = (i, {b"t": (True, True)})
        with pytest.raises(InvalidWitnessTransactionException):
            std_five.nodes[0].overlay.apply_witness_tx(blk, tx)

    def test_init_setup(self, std_five):
        assert (
            chr(RawBlockBroadcastPayload.msg_id) in std_five.nodes[0].overlay.decode_map
        )
        assert chr(BlockBroadcastPayload.msg_id) in std_five.nodes[0].overlay.decode_map

    def test_subscribe(self, std_five):
        assert std_five.nodes[0].overlay.is_subscribed(std_five.community_id)
        assert std_five.nodes[1].overlay.is_subscribed(std_five.community_id)
