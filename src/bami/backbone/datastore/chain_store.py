from abc import ABC, abstractmethod
import threading
from typing import Iterable, List, Optional, Set, Tuple

import cachetools

from bami.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.backbone.utils import (
    Dot,
    expand_ranges,
    GENESIS_DOT,
    GENESIS_HASH,
    Links,
    ranges,
    Ranges,
    shorten,
    ShortKey,
)


def _get_first(set_val):
    return {s[0] for s in set_val}


class BaseChain(ABC):
    @abstractmethod
    def add_block(
        self, block_links: Links, block_seq_num: int, block_hash: bytes
    ) -> Iterable[Dot]:
        pass

    @abstractmethod
    def reconcile(
        self, frontier: Frontier, last_reconcile_point: int = None
    ) -> FrontierDiff:
        """Reconcile with frontier wrt to last reconciled sequence number"""
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

    @abstractmethod
    def get_dots_by_seq_num(self, seq_num: int) -> Iterable[Dot]:
        pass

    @abstractmethod
    def get_all_short_hash_by_seq_num(self, seq_num: int) -> Optional[Set[ShortKey]]:
        pass


class BaseChainFactory(ABC):
    @abstractmethod
    def create_chain(self, **kwargs) -> BaseChain:
        pass


class Chain(BaseChain):
    def __init__(self, cache_num=10_000, max_extra_dots=5):
        """DAG-Chain of one community based on in-memory dicts.

        Args:
            cache_num: to store and support terminal calculation. Default= 100`000
        """
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
        self.terminal = Links(((0, shorten(GENESIS_HASH)),))

        self.const_terminal = self.terminal

        self.max_known_seq_num = 0
        self.max_extra_dots = max_extra_dots

        # Cache to speed up bfs on links
        self.term_cache = cachetools.LRUCache(cache_num)

        self.lock = threading.Lock()

    def get_all_short_hash_by_seq_num(self, seq_num: int) -> Optional[Set[ShortKey]]:
        return self.versions.get(seq_num)

    def get_dots_by_seq_num(self, seq_num: int) -> Optional[Iterable[Dot]]:
        if not self.versions.get(seq_num):
            return None
        for k in self.versions.get(seq_num):
            yield Dot((seq_num, k))

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

    def _is_block_dot_consistent(self, block_dot: Dot) -> bool:
        back_links = self.get_prev_links(block_dot)
        return back_links is not None and self._is_block_links_consistent(back_links)

    def consistency_fix(self, block_dot: Dot) -> Iterable[Dot]:
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

    def _add_inconsistencies(self, block_links: Links, block_dot: Dot) -> bool:
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

    def _remove_inconsistencies(
        self, block_dot: Dot, is_block_consistent: bool
    ) -> Iterable[Dot]:
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

    def add_block(
        self, block_links: Links, block_seq_num: int, block_hash: bytes
    ) -> List[Dot]:
        blk_hash = shorten(block_hash)
        block_dot = Dot((block_seq_num, blk_hash))

        with self.lock:
            # 0. Update versions
            self._update_versions(block_seq_num, blk_hash)
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
            self._update_terminal(block_seq_num, blk_hash, block_consistent)

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

    def reconcile(
        self, frontier: Frontier, last_reconcile_point: int = None
    ) -> FrontierDiff:

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

        # Check if peer has block that cover your inconsistencies
        for i in self.inconsistencies:
            for t in self.__calc_terminal(Links((i,))):
                if (
                    t in frontier.terminal
                    and t not in frontier.inconsistencies
                    and t[0] not in frontier.holes
                ):
                    conflicts.add(i)

        # from last reconcile point to
        if not last_reconcile_point:
            last_reconcile_point = 0
        extra_dots = {}
        # return FrontierDiff(missing, tuple(conflicts))
        # TODO: revisit this. How to choose the 'from' sequence number
        c = max(conflicts)
        last_point = last_reconcile_point if c[0] > last_reconcile_point else 0
        est_diff = c[0] - last_point
        mod_blk = round(est_diff / self.max_extra_dots)
        mod_blk = mod_blk + 1 if not mod_blk else mod_blk

        extra_val = {}
        for k in range(last_point + mod_blk, c[0] + 1, mod_blk):
            if self.versions.get(k):
                extra_val[k] = tuple(self.versions.get(k))
        extra_dots[c] = extra_val

        return FrontierDiff(missing, extra_dots)


class ChainFactory(BaseChainFactory):
    def create_chain(self, **kwargs) -> BaseChain:
        """ Args:
            cache_num: specify the cache number used in the chain
        """
        return Chain(**kwargs)
