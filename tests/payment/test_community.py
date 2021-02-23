from decimal import Decimal

from bami.backbone.exceptions import InvalidTransactionFormatException
from bami.backbone.payload import (
    BlockBroadcastPayload,
    RawBlockBroadcastPayload,
)
from bami.backbone.sub_community import IPv8SubCommunityFactory
from bami.backbone.utils import Dot, GENESIS_LINK
from bami.payment.community import PaymentCommunity
from bami.payment.exceptions import (
    InsufficientBalanceException,
    InvalidMintRangeException,
    InvalidSpendRangeException,
    InvalidAuditTransactionException,
    UnboundedMintException,
    UnknownMinterException,
)
import pytest

from tests.conftest import FakeBlock
from tests.mocking.base import deliver_messages


class FakePaymentCommunity(IPv8SubCommunityFactory, PaymentCommunity):
    pass


# Add tests for all exceptions
# Add tests on validity of transactions


@pytest.fixture(params=[5, 20])
def num_nodes(request):
    return request.param


@pytest.fixture
def overlay_class():
    return FakePaymentCommunity


@pytest.fixture
def init_nodes():
    return True


class TestInitCommunity:
    def test_empty(self, set_vals_by_nodes, num_nodes):
        nodes = set_vals_by_nodes.nodes
        assert len(nodes) == num_nodes

    def test_init_setup(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        assert nodes[0].overlay.decode_map[RawBlockBroadcastPayload.msg_id]
        assert nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]

    def test_subscribe(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        assert nodes[0].overlay.is_subscribed(set_vals_by_nodes.community_id)
        assert nodes[1].overlay.is_subscribed(set_vals_by_nodes.community_id)


class TestMint:
    def test_invalid_mint_tx_bad_format(self, set_vals_by_nodes):
        mint_tx = {}
        chain_id = set_vals_by_nodes.community_id
        minter = set_vals_by_nodes.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidTransactionFormatException):
            set_vals_by_nodes.nodes[0].overlay.verify_mint(chain_id, minter, mint_tx)

    def test_invalid_mint_tx_value_out_of_range(self, set_vals_by_nodes):
        mint_tx = {b"value": 10 ** 7}
        chain_id = set_vals_by_nodes.community_id
        minter = set_vals_by_nodes.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidMintRangeException):
            set_vals_by_nodes.nodes[0].overlay.verify_mint(chain_id, minter, mint_tx)

    def test_mint_value_unbound_value(self, set_vals_by_nodes):
        mint_tx = {b"value": 10 ** 7 - 1}
        chain_id = set_vals_by_nodes.community_id
        minter = set_vals_by_nodes.nodes[0].overlay.my_pub_key_bin
        set_vals_by_nodes.nodes[0].overlay.state_db.apply_mint(
            chain_id,
            Dot((1, "123123")),
            GENESIS_LINK,
            minter,
            Decimal(mint_tx.get(b"value"), set_vals_by_nodes.context),
            True,
        )
        next_mint = {b"value": 1}
        with pytest.raises(UnboundedMintException):
            set_vals_by_nodes.nodes[0].overlay.verify_mint(chain_id, minter, next_mint)

    @pytest.mark.asyncio
    async def test_invalid_mint(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        context = set_vals_by_nodes.context
        minter = nodes[1].overlay.my_pub_key_bin
        with pytest.raises(UnknownMinterException):
            nodes[1].overlay.mint(value=Decimal(10, context))
        await deliver_messages()
        n_nodes = len(set_vals_by_nodes.nodes)
        for i in range(n_nodes):
            assert nodes[i].overlay.state_db.get_balance(minter) == 0

    @pytest.mark.asyncio
    async def test_valid_mint(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        context = set_vals_by_nodes.context
        minter = nodes[0].overlay.my_pub_key_bin
        nodes[0].overlay.mint(value=Decimal(10, context))
        await deliver_messages(0.5)
        n_nodes = len(set_vals_by_nodes.nodes)
        for i in range(n_nodes):
            assert nodes[i].overlay.state_db.get_balance(minter) > 0


class TestSpend:
    @pytest.mark.asyncio
    async def test_invalid_spend(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        spender = vals.nodes[1].overlay.my_pub_key_bin
        with pytest.raises(InsufficientBalanceException):
            vals.nodes[1].overlay.spend(
                chain_id=vals.community_id,
                counter_party=vals.nodes[1],
                value=Decimal(10, vals.context),
            )

        await deliver_messages()
        # Should throw invalid mint exception
        n_nodes = len(set_vals_by_nodes.nodes)
        for i in range(n_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(spender) == 0

    def test_invalid_spend_bad_format(self, set_vals_by_nodes):
        spend_tx = {}
        minter = set_vals_by_nodes.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidTransactionFormatException):
            set_vals_by_nodes.nodes[0].overlay.verify_spend(minter, spend_tx)

    def test_invalid_spend_value_out_of_range(self, set_vals_by_nodes):
        spend_tx = {
            b"value": 10 ** 7,
            b"to_peer": set_vals_by_nodes.nodes[1].overlay.my_pub_key_bin,
            b"prev_pairwise_link": GENESIS_LINK,
        }
        spender = set_vals_by_nodes.nodes[0].overlay.my_pub_key_bin
        with pytest.raises(InvalidSpendRangeException):
            set_vals_by_nodes.nodes[0].overlay.verify_spend(spender, spend_tx)

    @pytest.mark.asyncio
    async def test_valid_spend(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        n_nodes = len(set_vals_by_nodes.nodes)
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

        await deliver_messages(0.1 * n_nodes)
        # Should throw invalid mint exception
        for i in range(n_nodes):
            assert (
                vals.nodes[i].overlay.state_db.get_balance(spender) == 0
            ), "Peer number {}".format(i)
            assert (
                vals.nodes[i].overlay.state_db.get_balance(counter_party) == 10
            ), "Peer number {}".format(i)
            assert not vals.nodes[i].overlay.state_db.was_balance_negative(spender)

    @pytest.mark.asyncio
    async def test_invalid_spend_ignore_validation(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        spender = vals.nodes[1].overlay.my_pub_key_bin
        vals.nodes[1].overlay.spend(
            chain_id=vals.community_id,
            counter_party=vals.nodes[0].overlay.my_pub_key_bin,
            value=Decimal(10, vals.context),
            ignore_validation=True,
        )
        assert vals.nodes[1].overlay.state_db.get_balance(spender) == -10
        n_nodes = len(vals.nodes)

        await deliver_messages(0.1 * n_nodes)
        # As the counterparty will reject the block => The spend transaction is reverted
        for i in range(n_nodes):
            assert vals.nodes[i].overlay.state_db.get_balance(spender) == 0
            assert vals.nodes[i].overlay.state_db.was_balance_negative(spender)


class TestAudit:
    # Test witness transaction
    def test_apply_invalid_audit_tx(self, set_vals_by_nodes):
        blk = FakeBlock(com_id=set_vals_by_nodes.community_id)
        i = 1
        set_vals_by_nodes.nodes[0].overlay.audit_delta = 100
        while set_vals_by_nodes.nodes[0].overlay.should_audit_chain_point(
            blk.com_id, blk.public_key, i
        ):
            i += 1
        tx = (i, {b"t": (True, True)})
        with pytest.raises(InvalidAuditTransactionException):
            set_vals_by_nodes.nodes[0].overlay.apply_audit_tx(blk, tx)
