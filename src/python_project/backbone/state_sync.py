from abc import ABCMeta
from typing import Any

from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_unsigned
from ipv8.peer import Peer
from python_project.backbone.community_routines import (
    MessageStateMachine,
    CommunityRoutines,
)
from python_project.backbone.datastore.utils import encode_raw
from python_project.backbone.payload import (
    StateRequestPayload,
    StateResponsePayload,
    StateByHashRequestPayload,
    StateByHashResponsePayload,
)


class StateSyncMixin(MessageStateMachine, CommunityRoutines, metaclass=ABCMeta):
    def setup_messages(self) -> None:
        self.add_message_handler(StateRequestPayload, self.received_state_request)
        self.add_message_handler(StateResponsePayload, self.received_state_response)
        self.add_message_handler(
            StateByHashRequestPayload, self.received_state_by_hash_request
        )

    def request_state(
        self,
        peer: Peer,
        chain_id: bytes,
        state_id: Any = None,
        include_other_witnesses: bool = True,
    ):
        self.logger.debug("Requesting state from a peer (%s) ", peer)
        state_request = {"state": state_id, "include_others": include_other_witnesses}
        self.send_packet(
            peer, StateRequestPayload(chain_id, encode_raw(state_request)), sig=True
        )

    @lazy_wrapper(StateRequestPayload)
    def received_state_request(self, peer: Peer, payload: StateRequestPayload):
        # Verify: 1) chain_id is known, 2) state_id is known, 3) take latest hash, state votes and respond
        pass

    @lazy_wrapper_unsigned(StateResponsePayload)
    def received_state_response(self, peer: Peer, payload: StateResponsePayload):
        """Verify the state, request state if required, send StateByHashRequest"""
        pass

    @lazy_wrapper(StateByHashRequestPayload)
    def received_state_by_hash_request(
        self, peer: Peer, payload: StateByHashRequestPayload
    ):
        """ Respond with state to the state_hash request"""
        pass
        # chain_id = payload.key
        # hash_val = payload.value
        # state = self.persistence.get_state_by_hash(chain_id, hash_val)
        # self.send_state_by_hash_response(source_address, chain_id, state)

    @lazy_wrapper_unsigned(StateByHashResponsePayload)
    def received_state_by_hash_response(
        self, peer: Peer, payload: StateByHashResponsePayload
    ):
        """ Dump state and add reaction? """
        pass
        # chain_id = payload.key
        # state, seq_num = json.loads(payload.value)
        # self.persistence.dump_state(chain_id, seq_num, state)

    # TODO: add periodical state sync
