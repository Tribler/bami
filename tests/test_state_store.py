from collections import defaultdict
from typing import Any

from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.state_store import (
    ChainState,
    StateStore,
    StateManager,
)
from python_project.backbone.datastore.utils import decode_raw


class SimpleCounter(ChainState):
    @property
    def is_delta_state(self):
        return False

    @property
    def is_personal_chain(self):
        return False

    def apply_block(self, prev_state, block: PlexusBlock):
        # Block is a mutator for the previous state
        id_val = decode_raw(block.transaction).get("id")
        return prev_state + id_val

    def init_state(self):
        return 0

    def merge(self, old_state, new_state):
        return max(old_state, new_state)


class DeltaCounter(ChainState):
    @property
    def is_delta_state(self):
        return False

    @property
    def is_personal_chain(self):
        return False

    def apply_block(self, prev_state, block):
        # Block is a mutator for the previous state
        return prev_state + 1

    def init_state(self):
        return 0

    def merge(self, old_state, new_state):
        return max(old_state, new_state)


class DictStore(StateStore):
    def __init__(self):
        self.dict_val = defaultdict(int)

    def get_last_state(self):
        return self.dict_val

    def insert(self, val: Any) -> bool:
        # Decode the transaction
        dec_val = decode_raw(val)
        self.dict_val[dec_val["id"]] += 1

    def init_state(self):
        pass


def test_add_state(create_batches, insert_function):
    state_code = SimpleCounter()

    mng = StateManager(None, DictStore())
    block_batch = create_batches(1)[0]
    insert_function(mng, block_batch)

    print(mng.get_latest_state())


def test_add_delta_state(create_batches, insert_function):
    state_code = DeltaCounter()
