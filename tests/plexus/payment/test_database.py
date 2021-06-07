from decimal import Decimal, getcontext

import cachetools
import pytest
from bami.plexus.backbone.utils import (
    decode_raw,
    Dot,
    encode_raw,
    GENESIS_LINK,
    Links,
    shorten,
    take_hash,
)
from bami.plexus.payment.database import ChainState, PaymentState
from bami.plexus.payment.exceptions import (
    InconsistentClaimException,
    InconsistentStateHashException,
    InvalidClaimException,
)


class TestPaymentState:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.prec = 10
        self.state = PaymentState(self.prec)
        self.minter = b"minter"
        self.con = getcontext()
        self.con.prec = self.prec

        self.spender = b"minter"
        self.receiver = b"test2"

    def test_add_mint(self):
        chain_id = self.minter
        value = Decimal(12.00, self.con)
        dot = Dot((1, "123123"))

        self.state.apply_mint(chain_id, dot, GENESIS_LINK, self.minter, value)
        assert self.state.get_balance(self.minter) == value

    def test_mint_and_spend(self):
        chain_id = self.minter
        value = Decimal(15.00, self.con)
        dot = Dot((1, "123123"))

        self.state.apply_mint(chain_id, dot, GENESIS_LINK, self.minter, value)
        assert self.state.get_balance(self.minter) == value
        assert not self.state.is_chain_forked(chain_id, self.minter)

        spend_value = Decimal(12.00, self.con)
        spend_dot = Dot((2, "23123"))
        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            Links((dot,)),
            spend_dot,
            self.spender,
            self.receiver,
            spend_value,
        )
        assert float(self.state.get_balance(self.spender)) == 3
        assert not self.state.is_chain_forked(chain_id, self.spender)
        assert not self.state.was_chain_forked(chain_id, self.spender)
        return chain_id, spend_value, spend_dot

    def test_valid_spend_with_confirm(self):
        chain_id, value, spend_dot = self.test_mint_and_spend()

        confirm_dot = Dot((3, "33333"))
        # Add confirmation from the counter-party
        self.state.apply_confirm(
            chain_id,
            self.receiver,
            Links((spend_dot,)),
            confirm_dot,
            self.spender,
            spend_dot,
            value,
        )
        # As the transaction is confirmed, inconsistency is resolved
        assert float(self.state.get_balance(self.spender)) == 3
        assert float(self.state.get_balance(self.receiver)) == value

    def test_valid_spend_with_reject(self):
        chain_id, value, spend_dot = self.test_mint_and_spend()

        reject_dot = Dot((3, "33333"))
        # Add confirmation from the counter-party
        self.state.apply_reject(
            chain_id,
            self.receiver,
            Links((spend_dot,)),
            reject_dot,
            self.spender,
            spend_dot,
        )
        # As the transaction is confirmed, inconsistency is resolved
        assert float(self.state.get_balance(self.spender)) == 3 + value
        assert float(self.state.get_balance(self.receiver)) == 0

    def test_risky_spend(self):
        self.chain_id = self.minter
        spend_value = Decimal(10.00, self.con)
        spend_dot = Dot((1, "11111"))
        self.state.apply_spend(
            self.chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            spend_dot,
            self.spender,
            self.receiver,
            spend_value,
        )
        assert float(self.state.get_balance(self.spender)) == -10
        # Next spend - tries to pretend as if hasn't happen
        self.new_spend_value = Decimal(6.00, self.con)
        self.new_spend_dot = Dot((2, "22222"))
        self.state.apply_spend(
            self.chain_id,
            Links((spend_dot,)),
            Links((spend_dot,)),
            self.new_spend_dot,
            self.spender,
            self.receiver,
            self.new_spend_value,
        )
        # As the value is less -> the balance will not change until confirmed, or rejected
        assert float(self.state.get_balance(self.spender)) == -10

    def test_risky_spend_with_confirm(self):
        self.test_risky_spend()

        confirm_dot = Dot((3, "33333"))
        # Add confirmation from the counter-party
        self.state.apply_confirm(
            self.chain_id,
            self.receiver,
            Links((self.new_spend_dot,)),
            confirm_dot,
            self.spender,
            self.new_spend_dot,
            self.new_spend_value,
        )
        # As the transaction is confirmed, inconsistency is resolved
        assert float(self.state.get_balance(self.spender)) == -6
        assert float(self.state.get_balance(self.receiver)) == 6
        assert self.state.was_balance_negative(self.spender)

    def test_risky_spend_with_reject(self):
        self.test_risky_spend()

        reject_dot = Dot((3, "33333"))
        # Add confirmation from the counter-party
        self.state.apply_reject(
            self.chain_id,
            self.receiver,
            Links((self.new_spend_dot,)),
            reject_dot,
            self.spender,
            self.new_spend_dot,
        )
        # As the transaction is rejected - the effect of it is reverted to the previous stable state - zero.
        assert float(self.state.get_balance(self.spender)) == 0
        assert float(self.state.get_balance(self.receiver)) == 0
        assert self.state.was_balance_negative(self.spender)

    def test_mint_and_spend_fork(self):
        chain_id = self.minter
        value = Decimal(12.00, self.con)
        dot = Dot((1, "123123"))

        self.state.apply_mint(chain_id, dot, GENESIS_LINK, self.minter, value)
        assert self.state.get_balance(self.minter) == value

        spend_value = Decimal(12.00, self.con)
        spend_dot = Dot((1, "323123"))
        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            spend_dot,
            self.spender,
            self.receiver,
            spend_value,
        )
        assert float(self.state.get_balance(self.spender)) == 0
        assert self.state.is_chain_forked(chain_id, self.spender)

    def test_add_claim(self):
        value = Decimal(12.11, self.con)
        dot = Dot((1, "123123"))
        chain_id = self.spender

        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            dot,
            self.spender,
            self.receiver,
            value,
        )
        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.spender)) == -12.11

        claim_dot = Dot((1, "2323"))

        self.state.apply_confirm(
            chain_id, self.receiver, Links((dot,)), claim_dot, self.spender, dot, value
        )
        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.receiver)) == 12.11

    def test_add_invalid_claim(self):
        value = Decimal(1, self.con)
        dot = Dot((1, "123123"))
        chain_id = self.spender

        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            dot,
            self.spender,
            self.receiver,
            value,
        )
        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.spender)) == -1

        new_dot = Dot((1, "223123"))
        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            new_dot,
            self.spender,
            self.receiver,
            value,
        )
        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.spender)) == -2
        assert self.state.was_chain_forked(chain_id, self.spender)

        claim_dot = Dot((2, "33323"))
        with pytest.raises(InvalidClaimException):
            # Should raise exception as the claim links are not correct
            self.state.apply_confirm(
                chain_id,
                self.receiver,
                GENESIS_LINK,
                claim_dot,
                self.spender,
                dot,
                value,
            )
        self.state.apply_confirm(
            chain_id, self.receiver, Links((dot,)), claim_dot, self.spender, dot, value
        )

        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.receiver)) == 1
        with pytest.raises(InvalidClaimException):
            # Double claim - should raise exception
            self.state.apply_confirm(
                chain_id,
                self.receiver,
                Links((claim_dot,)),
                claim_dot,
                self.spender,
                dot,
                value,
            )
        assert float(self.state.get_balance(self.receiver)) == 1
        assert not self.state.was_chain_forked(chain_id, self.receiver)

        # Add inconsistent claim
        inconsistent_value = Decimal(100, self.con)
        with pytest.raises(InconsistentClaimException):
            self.state.apply_confirm(
                chain_id,
                self.receiver,
                Links((claim_dot,)),
                claim_dot,
                self.spender,
                new_dot,
                inconsistent_value,
            )
        assert float(self.state.get_balance(self.receiver)) == 1
        assert not self.state.was_chain_forked(chain_id, self.receiver)

    def test_add_chain_state(self):
        chain_id = self.spender
        seq_num = 1
        state = ChainState({b"t1": (True, True)})
        state_hash = take_hash(state)
        self.state.add_chain_state(chain_id, seq_num, state_hash, state)
        self.state.prefered_statuses[chain_id][seq_num] = state_hash
        assert self.state.peer_statuses[chain_id][seq_num][state_hash] == state
        assert self.state.get_closest_peers_status(chain_id, 1) == (1, state)
        assert not self.state.get_closest_peers_status(chain_id, 2)

    def test_invalid_chain_state(self):
        # Add chain state with inconsistent hash
        chain_id = self.spender
        seq_num = 1
        state = ChainState({b"t1": (True, True)})
        state_hash = b"fake_hash"
        real_hash = take_hash(state)
        with pytest.raises(InconsistentStateHashException):
            self.state.add_chain_state(chain_id, seq_num, state_hash, state)
        self.state.add_chain_state(chain_id, seq_num, real_hash, state)
        assert not self.state.get_closest_peers_status(chain_id, 1)

    def test_get_peer_status_when_empty(self):
        chain_id = self.spender
        seq_num = 1
        assert not self.state.get_closest_peers_status(chain_id, seq_num)

    def test_spend_fork(self):
        value = Decimal(1, self.con)
        dot = Dot((1, b"123123"))
        chain_id = self.spender

        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            dot,
            self.spender,
            self.receiver,
            value,
        )
        assert float(self.state.get_balance(self.spender)) == -1
        new_dot = Dot((1, b"5464646"))
        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            GENESIS_LINK,
            new_dot,
            self.spender,
            self.receiver,
            value,
        )
        assert float(self.state.get_balance(self.spender)) == -2

        assert self.state.is_chain_forked(chain_id, self.spender)
        assert self.state.was_chain_forked(chain_id, self.spender)

        # Fix the fork:
        f_dot = Dot((2, b"000000"))
        value = Decimal(3, self.con)
        self.state.apply_spend(
            chain_id,
            Links((dot, new_dot)),
            Links((dot, new_dot)),
            f_dot,
            self.spender,
            self.receiver,
            value,
        )
        assert float(self.state.get_balance(self.spender)) == -3

        assert not self.state.is_chain_forked(chain_id, self.spender)
        assert self.state.was_chain_forked(chain_id, self.spender)

    def test_state_updates(self):
        chain_id = self.minter
        value = Decimal(12.00, self.con)
        dot = Dot((1, b"123123"))

        self.state.apply_mint(
            chain_id, dot, GENESIS_LINK, self.minter, value, store_update=True
        )
        assert self.state.get_balance(self.minter) == value
        assert not self.state.is_chain_forked(chain_id, self.minter)
        spend_value = Decimal(12.00, self.con)
        spend_dot = Dot((2, b"23123"))
        self.state.apply_spend(
            chain_id,
            GENESIS_LINK,
            Links((dot,)),
            spend_dot,
            self.spender,
            self.receiver,
            spend_value,
            store_status_update=True,
        )
        assert float(self.state.get_balance(self.spender)) == 0
        assert not self.state.is_chain_forked(chain_id, self.spender)

        claim_dot = Dot((3, b"23323"))

        self.state.apply_confirm(
            chain_id,
            self.receiver,
            Links((spend_dot,)),
            claim_dot,
            self.spender,
            spend_dot,
            spend_value,
            store_update=True,
        )

        v = self.state.get_closest_peers_status(chain_id, 1)
        assert v is not None
        assert v[0] == 1
        assert len(v[1]) == 1
        assert v[1].get(shorten(self.spender)) == (True, True)

        v = self.state.get_closest_peers_status(chain_id, 2)
        assert v is not None
        assert (
            (v[0] == 2)
            and (len(v[1]) == 1)
            and (v[1].get(shorten(self.spender)) == (True, True))
        )

        v = self.state.get_closest_peers_status(chain_id, 3)
        assert v is not None
        assert (
            v[0] == 3
            and len(v[1]) == 2
            and (v[1].get(shorten(self.spender)) == (True, True))
            and (v[1].get(shorten(self.receiver)) == (True, True))
        )

        assert v[1] == self.state.get_last_peer_status(chain_id)

    def test_minter_update(self):
        chain_id = b"chain"
        minters = {b"minter1", b"minter2"}
        self.state.add_known_minters(chain_id, minters)
        assert self.state.known_chain_minters(chain_id) == minters

    def test_val(self):
        val = Decimal(10, self.con)
        l = encode_raw({b"123": float(val)})
        d = decode_raw(l)
        print(d)

        l = cachetools.LRUCache(10)
        l[1] = "v"
        int_max = 9999
        k = 4
        try:
            v = min(l.keys(), key=lambda x: abs(x - k) if x >= k else int_max)
            if v < k:
                # No data yet:
                pass
        except ValueError:
            print("No data for the chain")
