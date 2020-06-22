from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional, List

from python_project.backbone.block import PlexusBlock, GENESIS_HASH
from python_project.backbone.datastore.block_store import BaseBlockStore
from python_project.backbone.datastore.database import BaseDB
from python_project.backbone.datastore.utils import (
    shorten,
    take_hash,
    Dot,
    encode_raw,
    decode_raw,
    Links,
)


class ChainState(ABC):
    """
    Interface for application logic for the state calculation.
    Class to collapse the chain and validate on integrity of invariants
    """

    @property
    @abstractmethod
    def is_personal_chain(self):
        pass

    @property
    @abstractmethod
    def is_delta_state(self):
        pass

    @abstractmethod
    def apply_block(self, prev_state, block) -> Any:
        """
        Apply block(with delta) to the prev_state

        Args:
            prev_state: Tuple of states which block links to old state
            block: block object
        Returns:
            New state with block applied
        """
        pass

    @abstractmethod
    def init_state(self) -> Any:
        """
        Initialize state when there no blocks
        Returns:
             Fresh initial state
        """
        pass

    @abstractmethod
    def merge(self, old_state, new_state) -> Any:
        """
        Merge two potentially conflicting states
        @param old_state:
        @param new_state:
        @return: Fresh new state of merged states
        """
        pass


class DeltaBasedState(ABC):
    @abstractmethod
    def init_state(self) -> Any:
        """
        Initialize state when there no blocks
        Returns:
             Fresh initial state
        """
        pass


class BaseStateStore(ABC):
    @abstractmethod
    def add_block(self, block: PlexusBlock) -> bool:
        pass


class StateStore(ABC):
    @abstractmethod
    def get_last_dot(self):
        pass

    @abstractmethod
    def get_last_dot_state(self):
        pass

    @abstractmethod
    def insert(self, val: Any) -> bool:
        pass

    @abstractmethod
    def init_state(self):
        pass


class ApplyMode(Enum):
    NO_ORDER = 1
    CASUAL_ORDER = 2
    LINEAR_ORDER = 3


class State:
    def can_be_applied(self, dot: Dot) -> bool:
        """Can the transaction be applied (consistency rules)"""
        return True

    def get_last_dot(self):
        pass

    def get_tx_by_dot(self, chain_id: bytes, dot: Dot) -> Optional[Any]:
        """Return transaction serialized"""
        tx_blob = self.back_db.get_tx_blob_by_dot(chain_id, dot)
        return decode_raw(tx_blob) if tx_blob else None

    def apply_tx(self, chain_id: bytes, prev_links: Links, dot: Dot, tx: Any) -> bool:
        """Return false if the transaction is not applied, rejected because of validity rules violation"""
        # Add your logic here
        return True

    def __init__(self, db_manager: BaseDB) -> None:
        self.back_db = db_manager

    def receive_chain_dots(self, chain_id: bytes, chain_dots: List):
        if not chain_dots or not chain_id:
            return

        # Go through personal chain dots
        for dot in chain_dots:
            # Follow the seque nce
            if self.can_be_applied(dot[0]):
                tx = self.get_tx_by_dot(chain_id, dot)
                chain = self.back_db.get_chain(chain_id)
                if not tx or not chain:
                    # TODO: throw an exception
                    pass
                else:
                    prev_links = chain.get_prev_links(dot)
                    self.apply_tx(chain_id, prev_links, dot, tx)


class StateManager(BaseStateStore):
    """
    Try different modes:
    1. No order
    2. Casual order
    3. Linear order
    """

    def add_block(self, block: PlexusBlock) -> bool:
        links = (
            block.previous
            if self.state_code and self.state_code.is_personal_chain
            else block.links
        )
        block_num = (
            block.sequence_number
            if self.state_code and self.state_code.is_personal_chain
            else block.com_seq_num
        )
        block_id = (block_num, block.short_hash)

        full_state = None

        # No order important
        return self.state.insert(block.transaction)

    def get_latest_state(self):
        return self.state.get_last_state()

    def __init__(
        self, chain_state: ChainState, state_store: StateStore, state_mode: ApplyMode
    ):
        self.state_code = chain_state
        init_block = (0, shorten(GENESIS_HASH))
        self.state = state_store
        self.state.init_state()

        self.apply_mode = state_mode


class FChain:
    """
    Index class for chain to ensure that each peer will converge into a consistent chain log.
    """

    def status_calc(self):
        if self.states and self.is_state_consistent():
            for sn, state in self.states.items():
                prev_state = self.state_checkpoints[sn].get(s - 1)
                if not prev_state:
                    # Previous state not known yet
                    break
                # take chain state class  and apply block
                known_state = self.state_checkpoints[sn].get(s)
                current_block = self.block_store.get_block_by_short_hash(h)
                new_state = state.apply_block(prev_state, current_block)
                merged_state = state.merge(known_state, new_state)
                self.state_checkpoints[sn][s] = merged_state

    def __init__(
        self, chain_id, personal=True, num_frontiers_store=50, block_store=None
    ):
        self.chain = dict()
        self.holes = set()

        self.chain_id = chain_id

        self.inconsistencies = set()
        self.terminal = set()

        self.personal = personal
        self.forward_pointers = dict()
        self.frontier = {"p": personal}

        self.last_const_state = None
        self.state_checkpoints = dict()
        self.hash_to_state = dict()

        self.states = dict()
        self.state_votes = dict()

        self.block_store = block_store

        self.num_front_store = num_frontiers_store

    def is_state_consistent(self):
        """
        State should be 'consistent' if there no known holes and inconsistencies
        """
        return not self.inconsistencies and not self.holes

    def add_state(self, chain_state):
        chain_state.versions = self
        chain_state.personal = self.personal
        self.states[chain_state.name] = chain_state

        # initialize zero state
        if chain_state.name not in self.state_checkpoints:
            self.state_checkpoints[chain_state.name] = dict()
        init_state = chain_state.init_state()
        self.state_checkpoints[chain_state.name][0] = init_state
        self.hash_to_state[take_hash(init_state)] = 0

    def max_known_seq_num(self):
        return max(self.chain) if self.chain else 0

    def clean_up(self):
        pass
        # TODO: implement cleanup for states and frontiers

    def get_latest_max_votes(self):
        return max(self.state_votes.items(), key=lambda x: (len(x[1]), x[0]))

    def get_latest_votes(self):
        return max(self.state_votes.items(), key=lambda x: x[0])

    def get_state_by_hash(self, state_hash):
        return self.hash_to_state.get(state_hash)

    def get_last_state(self):
        return {k: {max(v): v.get(max(v))} for k, v in self.state_checkpoints.items()}

    def get_state(self, seq_num, state_name=None):
        if state_name:
            return self.state_checkpoints.get(state_name).get(seq_num)
        else:
            # get all by seq_num
            return {k: v.get(seq_num) for k, v in self.state_checkpoints.items()}

    def add_state_vote(self, seq_num, state_vote):
        if seq_num not in self.state_votes:
            self.state_votes[seq_num] = set()
        self.state_votes[seq_num].add(state_vote)

    def reconcile(self, front):
        if "state" in front:
            # persist state val
            key = max(front["v"])[0]
            self.add_state_vote(key, tuple(front["state"]))

        f_holes = expand_ranges(front["h"]) if "h" in front and front["h"] else set()
        max_front_seq = max(front["v"])[0] if "v" in front and front["v"] else 0

        front_known_seq = expand_ranges([(1, max_front_seq)]) - f_holes
        peer_known_seq = expand_ranges([(1, self.max_known_seq_num())]) - self.holes

        # Front has blocks that peer is missing => Request from front these blocks
        f_diff = front_known_seq - peer_known_seq
        front_diff = {"m": ranges(f_diff)}

        if "v" in front:
            # Front has blocks with conflicting hash => Request these blocks
            front_diff["c"] = {
                (s, h)
                for s, h in front["v"]
                if s in self.chain and h not in self.chain[s]
            }

        for i in self.inconsistencies:
            for t in self.calc_terminal([i]):
                if t in front["v"] and t not in front["i"] and t[0] not in front["h"]:
                    front_diff["c"].add(i)

        return front_diff, None

    def add_block(self, block):
        block_links = block.previous if self.personal else block.links
        block_seq_num = block.sequence_number if self.personal else block.com_seq_num
        block_hash = shorten(block.hash)

        if block_seq_num not in self.chain:
            # new sequence number
            self.chain[block_seq_num] = set()

        self.chain[block_seq_num].add(block_hash)

        # analyze back pointers
        for s, h in block_links:
            if (s, h) not in self.forward_pointers:
                self.forward_pointers[(s, h)] = set()
            self.forward_pointers[(s, h)].add((block_seq_num, block_hash))

            if s in self.chain and h not in self.chain[s]:
                # previous block not present, but sibling is present => inconsistency
                self.add_inconsistency(s, h)

        # analyze forward pointers, i.e. inconsistencies
        if (block_seq_num, block_hash) in self.inconsistencies:
            # There exits a block that links to this => inconsistency fixed
            self.inconsistencies.remove((block_seq_num, block_hash))

        self._update_frontiers(block_links, block_seq_num, block_hash)

        # Update hash of the latest state
        if self.is_state_consistent():
            state_hash = take_hash(self.get_state(block_seq_num))
            if state_hash not in self.hash_to_state:
                self.hash_to_state[state_hash] = 0
            self.hash_to_state[state_hash] = max(
                self.hash_to_state[state_hash], block_seq_num
            )

        self.clean_up()
