from dataclasses import dataclass
from decimal import Decimal, getcontext

import cachetools
import pytest
from python_project.backbone.datastore.utils import (
    GENESIS_LINK,
    Dot,
    Links,
    encode_raw,
    decode_raw,
)
from python_project.payment.database import PaymentState


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
        value = Decimal(12.00, self.con)
        dot = Dot((1, "123123"))

        self.state.apply_mint(chain_id, dot, GENESIS_LINK, self.minter, value)
        assert self.state.get_balance(self.minter) == value
        assert not self.state.is_chain_forked(self.minter)

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
        assert float(self.state.get_balance(self.spender)) == 0
        assert not self.state.is_chain_forked(self.spender)

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
        assert self.state.is_chain_forked(self.spender)

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
            chain_id, self.receiver, GENESIS_LINK, claim_dot, self.spender, dot, value
        )
        assert self.state.last_spend_values[self.spender][self.receiver][dot] == value
        assert float(self.state.get_balance(self.receiver)) == 12.11

    def test_spend_fork(self):
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
        assert float(self.state.get_balance(self.spender)) == -1
        new_dot = Dot((1, "5464646"))
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

        assert self.state.is_chain_forked(self.spender)
        assert self.state.was_chain_forked(self.spender)

        # Fix the fork:
        f_dot = Dot((2, "000000"))
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

        assert not self.state.is_chain_forked(self.spender)
        assert self.state.was_chain_forked(self.spender)

    def test_state_updates(self):
        chain_id = self.minter
        value = Decimal(12.00, self.con)
        dot = Dot((1, "123123"))

        self.state.apply_mint(
            chain_id, dot, GENESIS_LINK, self.minter, value, store_update=True
        )
        assert self.state.get_balance(self.minter) == value
        assert not self.state.is_chain_forked(self.minter)
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
            store_status_update=True,
        )
        assert float(self.state.get_balance(self.spender)) == 0
        assert not self.state.is_chain_forked(self.spender)

        claim_dot = Dot((3, "2323"))

        self.state.apply_confirm(
            chain_id,
            self.receiver,
            GENESIS_LINK,
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
        assert v[1].get(self.spender) == (True, True)

        v = self.state.get_closest_peers_status(chain_id, 2)
        assert v is not None
        assert (
            (v[0] == 2)
            and (len(v[1]) == 1)
            and (v[1].get(self.spender) == (True, True))
        )

        v = self.state.get_closest_peers_status(chain_id, 3)
        assert v is not None
        assert (
            v[0] == 3
            and len(v[1]) == 2
            and (v[1].get(self.spender) == (True, True))
            and (v[1].get(self.receiver) == (True, True))
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
