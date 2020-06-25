from abc import ABC, abstractmethod
from typing import Any, Tuple

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.keys import Key
from ipv8.peer import Peer
from python_project.backbone.datastore.utils import take_hash, StateVote


class CommonRoutines(ABC):
    @property
    @abstractmethod
    def my_peer_key(self) -> Key:
        pass

    @property
    @abstractmethod
    def my_pub_key(self) -> bytes:
        pass

    @abstractmethod
    def send_packet(self, peer: Peer, packet: Any) -> None:
        pass


class StateRoutines(CommonRoutines, ABC):
    @property
    def crypto(self):
        return default_eccrypto

    def sign_state(self, state_blob: bytes) -> StateVote:
        """Sign state blob and return StateVote
        Args:
            state_blob: blob of state to sign
        Returns:
            Tuple with public key, signature and state hash
        """
        signature = self.crypto.create_signature(self.my_peer_key, state_blob)
        return StateVote((self.my_pub_key, signature, state_blob))

    def verify_state_vote(self, state_vote: StateVote) -> bool:
        # This is a claim of a conditional transaction
        pub_key, signature, state_blob = state_vote
        return self.crypto.is_valid_signature(
            self.crypto.key_from_public_bin(pub_key), state_blob, signature
        )
