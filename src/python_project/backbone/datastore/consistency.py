from python_project.backbone.datastore.utils import (
    key_to_id,
    ranges,
    expand_ranges,
    json_hash,
)


class ChainState:
    """
    Interface for application logic for the state calculation.
    Class to collapse the chain and validate on integrity of invariants
    """

    def __init__(self, name):
        self.name = name
        self.personal = False

    def apply_block(self, prev_state, block):
        """
        Apply block(with delta) to the prev_state
        @param prev_state:
        @param block:
        @return: Return new_state
        """
        return

    def init_state(self):
        """
        Initialize state when there no blocks
        @return: Fresh new state
        """
        return

    def merge(self, old_state, new_state):
        """
        Merge two potentially conflicting states
        @param old_state:
        @param new_state:
        @return: Fresh new state of merged states
        """
        return


class Chain:
    """
    Index class for chain to ensure that each peer will converge into a consistent chain log.
    """

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
        chain_state.chain = self
        chain_state.personal = self.personal
        self.states[chain_state.name] = chain_state

        # initialize zero state
        if chain_state.name not in self.state_checkpoints:
            self.state_checkpoints[chain_state.name] = dict()
        init_state = chain_state.init_state()
        self.state_checkpoints[chain_state.name][0] = init_state
        self.hash_to_state[json_hash(init_state)] = 0

    def add_audit_proof(self):
        pass

    def calc_terminal(self, current):
        terminal = set()
        for s, h in current:
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

            if (s, h) not in self.forward_pointers:
                # Terminal nodes achieved
                terminal.add((s, h))
                # update the state if any
            else:
                # make a bfs step
                terminal.update(self.calc_terminal(self.forward_pointers[(s, h)]))
        return terminal

    def add_inconsistency(self, seq_num, exp_hash):
        self.inconsistencies.add((seq_num, exp_hash))

    def _update_frontiers(self, block_links, block_seq_num, block_hash):
        # New block received
        # 1. Does it fix some known holes?
        if block_seq_num in self.holes:
            self.holes.remove(block_seq_num)

        # 2. Does it introduce new holes?
        for s, h in block_links:
            if s not in self.chain:
                while s not in self.chain and s >= 1:
                    self.holes.add(s)
                    s -= 1

        # 3. Does it change terminal nodes?
        self.terminal = self.calc_terminal(self.terminal)
        current = {(block_seq_num, block_hash)}
        self.terminal.update(self.calc_terminal(current))

        # Update frontier with holes, inconsistencies and terminal
        self.frontier["v"] = self.terminal
        self.frontier["h"] = ranges(self.holes)
        self.frontier["i"] = self.inconsistencies

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
        block_hash = key_to_id(block.hash)

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
            state_hash = json_hash(self.get_state(block_seq_num))
            if state_hash not in self.hash_to_state:
                self.hash_to_state[state_hash] = 0
            self.hash_to_state[state_hash] = max(
                self.hash_to_state[state_hash], block_seq_num
            )

        self.clean_up()
