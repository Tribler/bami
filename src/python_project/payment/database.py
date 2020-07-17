from collections import defaultdict
from decimal import Decimal, getcontext
from typing import Dict, Iterable, NewType, Optional, Set, Tuple

import cachetools

from python_project.backbone.datastore.utils import (
    Dot,
    GENESIS_DOT,
    Links,
    shorten,
    take_hash,
)
from python_project.payment.exceptions import (
    InconsistentClaimException,
    InconsistentStateHashException,
    InvalidClaimException,
)

ChainState = NewType("ChainState", Dict[bytes, Tuple[bool, bool]])


class PaymentState(object):
    def __init__(self, precision: int) -> None:

        self.precision = precision
        new_con = getcontext()
        new_con.prec = precision
        self.context = new_con

        # Frontiers for the chain invariants
        self.peer_frontiers = defaultdict(lambda: {GENESIS_DOT})
        # Last spend values: spender-claimer - value
        self.last_spend_values = defaultdict(
            lambda: defaultdict(lambda: {GENESIS_DOT: Decimal(0, self.context)})
        )

        # Values to keep spender-claimer - value
        cache_num = 10
        self.vals_cache = defaultdict(
            lambda: defaultdict(lambda: cachetools.LRUCache(cache_num))
        )

        # chain_id -> {
        # seq_num: {chain_id balance, is_forked},
        # other peers: {balance, is_forked} }

        # Store last reaction dot of the counter-party with the spender: cp-spender - dot
        self.claim_dict = defaultdict(lambda: defaultdict(lambda: GENESIS_DOT))
        # Last finalized pairwise balance counter-party - spender - value
        self.claim_vals = defaultdict(
            lambda: defaultdict(lambda: Decimal(0, self.context))
        )

        self.peer_mints = defaultdict(lambda: Decimal(0, self.context))

        self.known_minters = defaultdict(lambda: set())
        self.fork_attempts = defaultdict(lambda: set())

        self.chain_peers = defaultdict(lambda: set())

        # Chain_id: seq_num: hash: set of voters
        self.prefered_statuses = defaultdict(lambda: cachetools.LRUCache(100))
        self.peer_statuses = defaultdict(lambda: cachetools.LRUCache(100))
        self.witness_votes = defaultdict(lambda: cachetools.LRUCache(100))

        self.applied_dots = set()
        self.balance_invariants = defaultdict(lambda: True)

    def get_last_pairwise_links(self, spender: bytes, claimer: bytes) -> Links:
        return tuple(self.last_spend_values[spender][claimer].keys())

    def known_chain_minters(self, chain_id: bytes) -> Optional[Iterable[bytes]]:
        return self.known_minters.get(chain_id)

    def add_known_minters(self, chain_id: bytes, minters: Set[bytes]) -> None:
        self.known_minters[chain_id].update(minters)

    def _check_invariants(self, peer_id: bytes):
        if self.get_balance(peer_id) < 0:
            self.balance_invariants[peer_id] = False

    def _check_forking(self, peer_id: bytes, personal_links: Links, dot: Dot):
        # Check if is peer forking the chain
        for prev_dot in personal_links:
            if prev_dot in self.peer_frontiers[peer_id]:
                self.peer_frontiers[peer_id].remove(prev_dot)
        self.peer_frontiers[peer_id].add(dot)
        if len(self.peer_frontiers[peer_id]) > 1:
            self.fork_attempts[peer_id].add(tuple(self.peer_frontiers[peer_id]))

    def _store_status_update(self, dot: Dot, chain_id: bytes):
        seq_num = dot[0]
        status = self.get_last_peer_status(chain_id=chain_id)
        state_hash = take_hash(status)
        if not self.peer_statuses[chain_id].get(seq_num):
            self.peer_statuses[chain_id][seq_num] = dict()
        self.peer_statuses[chain_id][seq_num][state_hash] = status
        self.prefered_statuses[chain_id][seq_num] = state_hash

    def _update_chain_invariants(
        self,
        chain_id: bytes,
        peer_id: bytes,
        prev_links: Links,
        tx_dot: Dot,
        store_update: bool,
    ):
        self.chain_peers[chain_id].add(peer_id)
        self._check_forking(peer_id, prev_links, tx_dot)
        if store_update:
            self._store_status_update(tx_dot, chain_id)
        self._check_invariants(peer_id)

    def apply_spend(
        self,
        chain_id: bytes,
        prev_spend_links: Links,
        prev_chain_links: Links,
        spend_dot: Dot,
        spender: bytes,
        receiver: bytes,
        value: Decimal,
        store_status_update: bool = False,
    ) -> None:
        """Apply spend transaction to the state"""
        # apply spend to the personal chain

        # Iterate through last spend values and sum them up
        full_val = 0
        for dot in prev_spend_links:
            if dot in self.last_spend_values[spender][receiver]:
                next_val = self.last_spend_values[spender][receiver].pop(dot)
                next_val = next_val if next_val else 0
                full_val += next_val

        if value >= full_val:
            # The value is monotonically increasing => The update should be consistent => replace with the given value.
            self.last_spend_values[spender][receiver][spend_dot] = value
            self.vals_cache[spender][receiver][spend_dot] = value
        else:
            # TODO: revisit. There is inconsistency in the spend declaration => How to react?
            #  1. Store both new value and estimated
            self.last_spend_values[spender][receiver][spend_dot] = (value, full_val)
            self.vals_cache[spender][receiver][spend_dot] = (value, full_val)

        # spender changed the state of the chain =>
        self._update_chain_invariants(
            chain_id, spender, prev_chain_links, spend_dot, store_status_update
        )

    def apply_mint(
        self,
        chain_id: bytes,
        mint_dot: Dot,
        prev_links: Links,
        minter: bytes,
        value: Decimal,
        store_update: bool = False,
    ) -> None:
        """Apply mint transaction as it is to the state. Assumes that mint is valid!"""
        self.peer_mints[minter] += value
        # Minter changed the state of the chain =>
        self._update_chain_invariants(
            chain_id, minter, prev_links, mint_dot, store_update
        )

    def _verify_reaction(self, spend_dot: Dot, claimer: bytes, spender: bytes):
        # Check if we already applied this claim:
        if spend_dot <= self.claim_dict[claimer][spender]:
            raise InvalidClaimException(
                "Counter-party reaction with link {spend_dot} already applied. Current frontier: {current}".format(
                    spend_dot=spend_dot, current=self.claim_dict[claimer][spender]
                )
            )

    def apply_confirm(
        self,
        chain_id: bytes,
        claimer: bytes,
        prev_links: Links,
        claim_dot: Dot,
        spender: bytes,
        spend_dot: Dot,
        value: Decimal,
        store_update: bool = False,
    ) -> None:
        """Apply confirm transaction to the state. Might raise exceptions if confirm is not valid:
            - Too old or inconsistent with the spend
        """
        # 1. Check if the confirm or reject is too old?
        self._verify_reaction(spend_dot, claimer, spender)
        # 2. Check if claim is consistent with the spend value
        val = self.vals_cache[spender][claimer].get(spend_dot)
        if not val or (type(val) == tuple and val[0] != value):
            raise InconsistentClaimException(
                "Claim from {peer} on chain {chain_id} with value {value} invalid! Spend value: {val}".format(
                    peer=claimer, chain_id=chain_id, value=value, val=val
                )
            )
        # Counter-parties agreed => Fix any inconsistencies introduced
        if type(val) == tuple and val[0] == value:
            self.vals_cache[spender][claimer][spend_dot] = value
            if spend_dot in self.last_spend_values[spender][claimer]:
                self.last_spend_values[spender][claimer][spend_dot] = value
        # Link this claim to the spend value
        self.claim_dict[claimer][spender] = spend_dot
        self.claim_vals[claimer][spender] = value

        self._update_chain_invariants(
            chain_id, claimer, prev_links, claim_dot, store_update
        )

    def apply_reject(
        self,
        chain_id: bytes,
        claimer: bytes,
        prev_links: Links,
        reject_dot: Dot,
        spender: bytes,
        spend_dot: Dot,
        store_update: bool = False,
    ):
        """Apply reject transaction to the state. Will raise exception if reject is too old"""
        self._verify_reaction(spend_dot, claimer, spender)
        # This reaction rejects this spend_dot and leaves the value as it is.
        self.claim_dict[claimer][spender] = spend_dot
        # Update the spend value => revert to previous finalized
        if spend_dot in self.last_spend_values[spender][claimer]:
            self.last_spend_values[spender][claimer][spend_dot] = self.claim_vals[
                claimer
            ][spender]
        # Update chain invariants
        self._update_chain_invariants(
            chain_id, claimer, prev_links, reject_dot, store_update
        )

    def get_total_spend(self, peer_id: bytes) -> Decimal:
        spends = Decimal(0, self.context)
        for _, val_dict in self.last_spend_values[peer_id].items():
            for dot, val in val_dict.items():
                if type(val) == tuple:
                    # value is inconsistent => take the max
                    spends = spends + max(val)
                else:
                    spends = spends + val
        return spends

    def get_total_claims(self, peer_id: bytes) -> Decimal:
        total_claim = Decimal(0, self.context)
        for _, val in self.claim_vals[peer_id].items():
            total_claim = total_claim + val
        return total_claim

    def get_balance(self, peer_id: bytes) -> Decimal:
        return (
            self.peer_mints[peer_id]
            + self.get_total_claims(peer_id)
            - self.get_total_spend(peer_id)
        )

    def was_balance_negative(self, peer_id: bytes) -> bool:
        return not self.balance_invariants[peer_id]

    def is_chain_forked(self, peer_id: bytes) -> bool:
        return len(self.peer_frontiers[peer_id]) > 1

    def was_chain_forked(self, peer_id: bytes) -> bool:
        return len(self.fork_attempts[peer_id]) >= 1

    # ----- For auditing and witnessing ---------
    def get_last_peer_status(self, chain_id: bytes) -> ChainState:
        """Get last balance of peers in the community"""
        v = dict()
        for p in self.chain_peers[chain_id]:
            v[shorten(p)] = (self.get_balance(p) >= 0, not self.is_chain_forked(p))
        return ChainState(v)

    def add_witness_vote(
        self, chain_id: bytes, seq_num: int, state_hash: bytes, witness_id: bytes
    ) -> None:
        prev_values = self.witness_votes[chain_id].get(seq_num)
        if not prev_values:
            self.witness_votes[chain_id][seq_num] = defaultdict(lambda: set())
        self.witness_votes[chain_id][seq_num][state_hash].add(witness_id)
        # TODO: add reaction if there is inconsistency
        state_hash = max(
            self.witness_votes[chain_id][seq_num].items(), key=lambda x: len(x[1])
        )[0]
        self.prefered_statuses[chain_id][seq_num] = state_hash

    def add_chain_state(
        self, chain_id: bytes, seq_num: int, state_hash: bytes, state: ChainState
    ) -> None:
        calc_hash = take_hash(state)
        if calc_hash != state_hash:
            raise InconsistentStateHashException(
                "State hash not equal", state_hash, calc_hash
            )
        if not self.peer_statuses[chain_id].get(seq_num):
            self.peer_statuses[chain_id][seq_num] = dict()
        self.peer_statuses[chain_id][seq_num][state_hash] = state

    def get_closest_peers_status(
        self, chain_id: bytes, seq_num: int
    ) -> Optional[Tuple[int, ChainState]]:
        int_max = 9999
        try:
            v = min(
                self.peer_statuses[chain_id].keys(),
                key=lambda x: abs(x - seq_num) if x >= seq_num else int_max,
            )
            if v < seq_num:
                # No data with seq_num yet
                return None
            else:
                # choose state hash - last own?
                # get preferred status
                state_hash = self.prefered_statuses[chain_id][v]
                return v, self.peer_statuses[chain_id][v][state_hash]
        except ValueError:
            # No data in the chain
            return None
