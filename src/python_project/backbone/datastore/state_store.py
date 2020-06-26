from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional, List

from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.database import BaseDB
from python_project.backbone.datastore.utils import (
    Dot,
    decode_raw,
    Links,
    StateVote,
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

    def get_last_state_blob(self) -> Optional[bytes]:
        """
        Get latest state blob that summarizing all state.
        Returns: None if is not applicable
        """
        return None

    def add_state_vote(self, links: Links, state_vote: StateVote) -> None:
        pass

    def receive_chain_dots(self, chain_id: bytes, chain_dots: List) -> None:
        if not chain_dots or not chain_id:
            return

        # Go through personal chain dots
        for dot in chain_dots:
            # Follow the sequence of dots
            if self.can_be_applied(dot[0]):
                tx = self.get_tx_by_dot(chain_id, dot)
                chain = self.back_db.get_chain(chain_id)
                if not tx or not chain:
                    # TODO: throw an exception
                    pass
                else:
                    prev_links = chain.get_prev_links(dot)
                    self.apply_tx(chain_id, prev_links, dot, tx)


class FChain:
    """
    Index class for chain to ensure that each peer will converge into a consistent chain log.
    """

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
