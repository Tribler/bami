import operator
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, Optional, Set

import cachetools
from python_project.backbone.block import PlexusBlock
from python_project.backbone.datastore.utils import (
    shorten,
    ranges,
    expand_ranges,
    take_hash,
    Links,
    decode_raw,
    encode_raw,
    Ranges,
    ShortKey,
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


@dataclass
class Frontier:
    vector: Links
    holes: Ranges
    inconsistencies: Links

    def to_bytes(self) -> bytes:
        return encode_raw(
            {"v": self.vector, "h": self.holes, "i": self.inconsistencies}
        )

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        front_dict = decode_raw(bytes_frontier)
        return cls(front_dict.get("v"), front_dict.get("h"), front_dict.get("i"))


@dataclass
class FrontierDiff:
    missing: Ranges
    conflicts: Links

    def to_bytes(self) -> bytes:
        return encode_raw({"m": self.missing, "c": self.conflicts})

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        val_dict = decode_raw(bytes_frontier)
        return cls(val_dict.get("m"), val_dict.get("c"))


class BaseChain(ABC):
    @abstractmethod
    def add_block(self, block: PlexusBlock) -> None:
        pass

    @abstractmethod
    def reconcile(self, frontier: Frontier) -> FrontierDiff:
        pass


class Chain(BaseChain):
    def __init__(self, is_personal_chain=False, cache_num=100_000):
        self.personal = is_personal_chain

        # Internal chain store of short hashes
        self.chain = dict()
        # Pointers to forward blocks
        self.forward_pointers = dict()
        # Known data structure inconsistencies
        self.inconsistencies = set()
        # Unknown blocks in the data structure
        self.holes = set()

        # Current data frontier
        self.frontier = None  # Frontier(None, None, None)
        # Current terminal nodes in the DAG
        self.terminal = Links(((0, ShortKey("30303030")),))

        self.last_const_state = None
        self.state_checkpoints = dict()
        self.hash_to_state = dict()

        self.max_known_seq_num = 0

        # Cache to speed up bfs on links
        self.term_cache = cachetools.LRUCache(cache_num)

    def _add_inconsistency(self, seq_num: int, exp_hash: ShortKey) -> None:
        """"""
        self.inconsistencies.add((seq_num, exp_hash))

    def _fix_holes(self, block_seq_num: int) -> None:
        """Fix any known holes """
        if block_seq_num in self.holes:
            self.holes.remove(block_seq_num)

    def _check_new_holes(self, block_links: Links) -> None:
        """Add hole if block_links not known"""
        for s, h in block_links:
            if s not in self.chain:
                while s not in self.chain and s >= 1:
                    self.holes.add(s)
                    s -= 1

    def get_next_link(self, link: Tuple[int, ShortKey]) -> Optional[Links]:
        val = self.forward_pointers.get(link)
        return Links(tuple(val)) if val else None

    def __calc_terminal2(self, current: Links) -> Set[Tuple[int, ShortKey]]:
        """Recursive iteration through the block links"""
        terminal = set()
        for blk_link in current:
            if blk_link not in self.forward_pointers:
                # Terminal nodes achieved
                terminal.add(blk_link)
            else:
                # Next blocks are available, check if there is cache
                cached_next = self.term_cache.get(blk_link, default=None)
                if cached_next:
                    # Cached next exits
                    new_cache = None

                    for cached_val in cached_next:
                        term_next = self.get_next_link(cached_val)
                        if not term_next:
                            # This is terminal node - update
                            terminal.update(cached_next)
                        else:
                            # This is not terminal, make next step and invalidate the cache
                            new_val = self.__calc_terminal2(term_next)
                            if not new_cache:
                                new_cache = set()
                            new_cache.update(new_val)
                            terminal.update(new_val)
                    if new_cache:
                        self.term_cache[blk_link] = new_cache
                else:
                    # No cache, make step and update cache
                    next_blk = self.get_next_link(blk_link)
                    new_term = self.__calc_terminal2(next_blk)
                    self.term_cache[blk_link] = new_term
                    terminal.update(new_term)
        return terminal

    @cachetools.cachedmethod(operator.attrgetter("term_cache"))
    def __calc_terminal(self, current: Links) -> Links:
        """Recursive iteration through the block links"""
        terminal = set()
        for s, h in current:
            if (s, h) not in self.forward_pointers:
                # Terminal nodes achieved
                terminal.add((s, h))
                # update the state if any
            else:
                # make a bfs step
                terminal.update(
                    self.__calc_terminal(Links(tuple(self.forward_pointers[(s, h)])))
                )
        return Links(tuple(terminal))

    # noinspection PyTypeChecker
    def _recalc_terminal(self, block_seq_num: int, block_short_hash: ShortKey) -> None:
        """Update current terminal nodes wrt new block"""

        # Check if the terminal nodes changed
        current_links = Links(((block_seq_num, block_short_hash),))
        # self.terminal = set()

        # val = self.term_cache.pop((self.terminal,), default=None)
        # Start traversal from the block
        new_term = self.__calc_terminal2(current_links)

        # Traversal from the current terminal nodes. Block can change the current terminal
        new_term.update(self.__calc_terminal2(self.terminal))
        new_term = sorted(new_term)
        self.terminal = Links(tuple(new_term))

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

    def _update_frontiers(self, block_links, block_seq_num, block_hash):
        # New block received
        # 1. Does it fix some known holes?

        # 2. Does it introduce new holes?

        # 3. Does it change terminal nodes?
        self.terminal = self._calc_terminal(self.terminal)
        current = {(block_seq_num, block_hash)}
        self.terminal.update(self.calc_terminal(current))

        # Update frontier with holes, inconsistencies and terminal
        self.frontier["v"] = self.terminal
        self.frontier["h"] = ranges(self.holes)
        self.frontier["i"] = self.inconsistencies

    def add_block(self, block: PlexusBlock) -> None:
        block_links = block.previous if self.personal else block.links
        block_seq_num = block.sequence_number if self.personal else block.com_seq_num
        block_hash = shorten(block.hash)

        if block_seq_num not in self.chain:
            # new sequence number
            self.chain[block_seq_num] = set()
            if block_seq_num > self.max_known_seq_num:
                self.max_known_seq_num = block_seq_num

        self.chain[block_seq_num].add(block_hash)

        # Analyze inconsistencies and update the forward pointers
        for seq, hash_val in block_links:
            if (seq, hash_val) not in self.forward_pointers:
                self.forward_pointers[(seq, hash_val)] = set()
            self.forward_pointers[(seq, hash_val)].add((block_seq_num, block_hash))

            if seq in self.chain and hash_val not in self.chain[seq]:
                self._add_inconsistency(seq, hash_val)

        # Check if block fixes some inconsistencies
        if (block_seq_num, block_hash) in self.inconsistencies:
            # There exits a block that links to this => inconsistency fixed
            self.inconsistencies.remove((block_seq_num, block_hash))
        # self._update_frontiers(block_links, block_seq_num, block_hash)

    def reconcile(self, frontier: Frontier) -> FrontierDiff:
        pass


class FChain(BaseChain):
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
