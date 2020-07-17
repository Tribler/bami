from decimal import Decimal

from ipv8.test.mocking.endpoint import internet
import pytest
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
    UnknownMinterException,
)

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
        # Should throw invalid mint exception
        for i in range(vals.num_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(minter) == 0

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

    def test_init_setup(self, std_five):
        assert (
            chr(RawBlockBroadcastPayload.msg_id) in std_five.nodes[0].overlay.decode_map
        )
        assert chr(BlockBroadcastPayload.msg_id) in std_five.nodes[0].overlay.decode_map

    def test_subscribe(self, std_five):
        assert std_five.nodes[0].overlay.is_subscribed(std_five.community_id)
        assert std_five.nodes[1].overlay.is_subscribed(std_five.community_id)
