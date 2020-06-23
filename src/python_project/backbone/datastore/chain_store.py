import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, Optional, Set, Any, List

import cachetools
from python_project.backbone.datastore.utils import (
    shorten,
    ranges,
    expand_ranges,
    Links,
    decode_raw,
    encode_raw,
    Ranges,
    ShortKey,
    Dot,
    GENESIS_DOT,
)


@dataclass
class Frontier:
    terminal: Links
    holes: Ranges
    inconsistencies: Links

    def to_bytes(self) -> bytes:
        return encode_raw(
            {"t": self.terminal, "h": self.holes, "i": self.inconsistencies}
        )

    @classmethod
    def from_bytes(cls, bytes_frontier: bytes):
        front_dict = decode_raw(bytes_frontier)
        return cls(front_dict.get("t"), front_dict.get("h"), front_dict.get("i"))


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


def _get_first(set_val):
    return {s[0] for s in set_val}


class BaseChain(ABC):
    @abstractmethod
    def add_block(self, block: Any) -> Dot:
        pass

    @abstractmethod
    def reconcile(self, frontier: Frontier) -> FrontierDiff:
        pass

    @property
    @abstractmethod
    def frontier(self) -> Frontier:
        pass

    @property
    @abstractmethod
    def consistent_terminal(self) -> Links:
        pass

    @abstractmethod
    def get_next_links(self, block_dot: Dot) -> Optional[Links]:
        pass

    @abstractmethod
    def get_prev_links(self, block_dot: Dot) -> Optional[Links]:
        pass


class BaseChainFactory(ABC):
    @abstractmethod
    def create_personal_chain(self, **kwargs) -> BaseChain:
        pass

    @abstractmethod
    def create_community_chain(self, **kwargs) -> BaseChain:
        pass


class Chain(BaseChain):
    def __init__(self, is_personal_chain=False, cache_num=10_000):
        """DAG-Chain of one community based on in-memory dicts.

        Args:
            is_personal_chain: if the chain must follow personal links (previous). Default: False
            cache_num: to store and support terminal calculation. Default= 100`000
        """
        self.personal = is_personal_chain

        # Internal chain store of short hashes
        self.versions = dict()
        # Pointers to forward blocks
        self.forward_pointers = dict()
        # Pointers to back blocks
        self.back_pointers = dict()

        # Known data structure inconsistencies
        self.inconsistencies = set()

        self.inconsistent_blocks = set()
        # Unknown blocks in the data structure
        self.holes = set()
        # Current terminal nodes in the DAG
        self.terminal = Links(((0, ShortKey("30303030")),))

        self.const_terminal = self.terminal

        self.max_known_seq_num = 0
        # Cache to speed up bfs on links
        self.term_cache = cachetools.LRUCache(cache_num)

        self.lock = threading.Lock()

    @property
    def consistent_terminal(self) -> Links:
        return self.const_terminal

    def get_next_links(self, dot: Dot) -> Optional[Links]:
        """Get forward link from the point.

        Args:
            dot: tuple of sequence number and short hash key

        Returns:
            A tuple of links
        """
        val = self.forward_pointers.get(dot)
        return Links(tuple(val)) if val else None

    def get_prev_links(self, dot: Dot) -> Optional[Links]:
        val = self.back_pointers.get(dot)
        return val if val else None

    def _update_holes(self, block_seq_num: int) -> None:
        """Fix known holes, or add any new"""
        # Check if this block fixes known holes
        if block_seq_num in self.holes:
            self.holes.remove(block_seq_num)
        # Check if block introduces new holes
        self.holes.update({i for i in range(self.max_known_seq_num + 1, block_seq_num)})
        self.max_known_seq_num = max(self.max_known_seq_num, block_seq_num)

    def _is_block_links_consistent(self, block_links: Links) -> bool:
        # Add to inconsistencies any unknown back pointers. If any block is not consistent
        return all(
            (dot == GENESIS_DOT or self.get_prev_links(dot) is not None)
            and dot not in self.inconsistent_blocks
            for dot in block_links
        )

    def _is_block_dot_consistent(self, block_dot: Dot):
        back_links = self.get_prev_links(block_dot)
        return back_links is not None and self._is_block_links_consistent(back_links)

    def consistency_fix(self, block_dot: Dot):
        if block_dot in self.inconsistent_blocks:
            self.inconsistent_blocks.remove(block_dot)
            yield block_dot
        next_links = self.get_next_links(block_dot)
        while next_links:
            next_val = set()
            for dot in next_links:
                if self._is_block_dot_consistent(dot):
                    # Block dot is consistent
                    next_dot_links = self.get_next_links(dot)
                    if next_dot_links:
                        next_val.update(set(next_dot_links))
                    if dot in self.inconsistent_blocks:
                        self.inconsistent_blocks.remove(dot)
                        yield dot
            next_links = next_val

    def _add_inconsistencies(
        self, block_links: Links, block_dot: Dot, update_trigger: Any = None
    ) -> bool:
        """Fix any inconsistencies in the data structure, and verify any new"""

        # Check if block introduces new inconsistencies
        is_block_consistent = True
        # Add to inconsistencies any unknown back pointers. If any block is not consistent
        for dot in block_links:
            if dot != GENESIS_DOT and not self.get_prev_links(dot):
                self.inconsistencies.add(dot)
                is_block_consistent = False

            # If the back pointer is not consistent
            if dot in self.inconsistent_blocks:
                is_block_consistent = False

        if not is_block_consistent:
            self.inconsistent_blocks.add(block_dot)

        return is_block_consistent

    def _remove_inconsistencies(self, block_dot: Dot, is_block_consistent: bool):
        # Check if block fixes some inconsistencies
        if block_dot in self.inconsistencies:
            self.inconsistencies.remove(block_dot)
            # Block might fixed some inconsistencies
            if is_block_consistent:
                yield block_dot
                for next_dot in self.get_next_links(block_dot):
                    if self._is_block_dot_consistent(next_dot):
                        yield from self.consistency_fix(next_dot)

    def __calc_terminal(
        self, current: Links, make_consistent_step: bool = False
    ) -> Set[Tuple[int, ShortKey]]:
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
                        if not make_consistent_step or cached_val[1]:
                            term_next = self.get_next_links(cached_val[0])
                            if not term_next:
                                # This is terminal node - update
                                terminal.update(_get_first(cached_next))
                            else:
                                # This is not terminal, make next step and invalidate the cache
                                new_val = self.__calc_terminal(
                                    term_next, make_consistent_step
                                )
                                if not new_cache:
                                    new_cache = set()
                                val_with_const = {
                                    (dot, self._is_block_dot_consistent(dot))
                                    for dot in new_val
                                }
                                new_cache.update(val_with_const)
                                terminal.update(new_val)
                    if new_cache:
                        self.term_cache[blk_link] = new_cache
                else:
                    # No cache, make step and update cache
                    next_blk = self.get_next_links(blk_link)
                    new_term = self.__calc_terminal(next_blk, make_consistent_step)
                    val_with_const = {
                        (dot, self._is_block_dot_consistent(dot)) for dot in new_term
                    }
                    self.term_cache[blk_link] = val_with_const
                    terminal.update(new_term)
        return terminal

    # noinspection PyTypeChecker
    def _update_terminal(
        self,
        block_seq_num: int,
        block_short_hash: ShortKey,
        consistent_update: bool = True,
    ) -> None:
        """Update current terminal nodes wrt new block"""

        # Check if the terminal nodes changed
        current_links = Links(((block_seq_num, block_short_hash),))
        # Start traversal from the block
        new_term = self.__calc_terminal(current_links)
        # Traversal from the current terminal nodes. Block can change the current terminal

        if consistent_update:
            const_step = self.__calc_terminal(
                self.const_terminal, make_consistent_step=True
            )
            const_step.update(new_term)
            const_step = sorted(const_step)
            self.const_terminal = Links(tuple(const_step))

        new_term.update(self.__calc_terminal(self.terminal))
        new_term = sorted(new_term)
        self.terminal = Links(tuple(new_term))

    def _update_forward_pointers(self, block_links: Links, block_dot: Dot) -> None:
        for dot in block_links:
            if dot not in self.forward_pointers:
                self.forward_pointers[dot] = set()
            self.forward_pointers[dot].add(block_dot)

    def _update_back_pointers(self, block_dot: Dot, block_links: Links):
        self.back_pointers[block_dot] = block_links

    def _update_versions(self, block_seq_num: int, block_hash: ShortKey) -> None:
        if block_seq_num not in self.versions:
            self.versions[block_seq_num] = set()
        self.versions[block_seq_num].add(block_hash)

    def add_block(self, block: Any) -> List[Dot]:
        block_links = block.previous if self.personal else block.links
        block_seq_num = block.sequence_number if self.personal else block.com_seq_num
        block_hash = shorten(block.hash)
        block_dot = Dot((block_seq_num, block_hash))

        with self.lock:
            # 1. Update versions
            self._update_versions(block_seq_num, block_hash)
            # 1. Update back pointers
            self._update_back_pointers(block_dot, block_links)
            # 2. Update forward pointers
            self._update_forward_pointers(block_links, block_dot)
            # 3. Update holes
            self._update_holes(block_seq_num)
            # 4. Update inconsistencies
            block_consistent = self._add_inconsistencies(block_links, block_dot)
            missing = list(self._remove_inconsistencies(block_dot, block_consistent))
            if missing:
                last_dot = missing[-1]
                for dot in missing[:-1]:
                    self.term_cache[Links((dot,))] = (Links((last_dot,)), True)
            # 5. Update terminal nodes if consistent
            old_terminal = self.const_terminal
            self._update_terminal(block_seq_num, block_hash, block_consistent)

        diff = set(self.consistent_terminal) - set(old_terminal)
        if diff and missing:
            # return list(itertools.chain([max(diff)], missing))
            return missing
        elif missing:
            return missing
        elif diff:
            return [max(diff)]
        else:
            return []

    @property
    def frontier(self) -> Frontier:
        with self.lock:
            return Frontier(
                self.terminal,
                ranges(self.holes),
                Links(tuple(sorted(self.inconsistencies))),
            )

    def reconcile(self, frontier: Frontier) -> FrontierDiff:

        f_holes = expand_ranges(frontier.holes)
        max_term_seq = max(frontier.terminal)[0]

        front_known_seq = expand_ranges(Ranges(((1, max_term_seq),))) - f_holes
        peer_known_seq = (
            expand_ranges(Ranges(((1, self.max_known_seq_num),))) - self.holes
        )

        # External frontier has blocks that peer is missing => Request from front these blocks
        f_diff = front_known_seq - peer_known_seq
        missing = ranges(f_diff)

        # Front has blocks with conflicting hash => Request these blocks
        conflicts = {
            (s, h)
            for s, h in frontier.terminal
            if s in self.versions and h not in self.versions[s]
        }

        for i in self.inconsistencies:
            for t in self.__calc_terminal(Links((i,))):
                if (
                    t in frontier.terminal
                    and t not in frontier.inconsistencies
                    and t[0] not in frontier.holes
                ):
                    conflicts.add(i)

        conflicts = Links(tuple(conflicts))

        return FrontierDiff(missing, conflicts)


class ChainFactory(BaseChainFactory):
    def create_personal_chain(self, **kwargs) -> BaseChain:
        """ **kwargs: chache_num: specify the cache number used in the chain
        """
        return Chain(is_personal_chain=True, **kwargs)

    def create_community_chain(self, **kwargs) -> BaseChain:
        """ **kwargs:chache_num: specify the cache number used in the chain
        """
        return Chain(is_personal_chain=False, **kwargs)
