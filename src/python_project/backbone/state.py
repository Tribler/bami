from abc import ABCMeta, ABC, abstractmethod
from typing import Optional

from python_project.backbone.community_routines import CommunityRoutines
from python_project.backbone.datastore.state_store import State
from python_project.backbone.datastore.utils import StateVote


class StateRoutines(ABC):
    @abstractmethod
    def get_cumulative_state_blob(self, subcom_id: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_state(self, subcom_id: bytes) -> Optional[State]:
        pass


class StateMixin(CommunityRoutines, metaclass=ABCMeta):
    def sign_state(self, state_blob: bytes) -> StateVote:
        """Sign state blob and return StateVote
        Args:
            state_blob: blob of state to sign
        Returns:
            Tuple with public key, signature and state hash
        """
        signature = self.crypto.create_signature(self.my_peer_key, state_blob)
        return StateVote((self.my_pub_key_bin, signature, state_blob))

    def verify_state_vote(self, state_vote: StateVote) -> bool:
        # This is a claim of a conditional transaction
        pub_key, signature, state_blob = state_vote
        return self.crypto.is_valid_signature(
            self.crypto.key_from_public_bin(pub_key), state_blob, signature
        )
