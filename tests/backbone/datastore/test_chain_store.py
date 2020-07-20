from itertools import chain

import pytest
from python_project.backbone.datastore.chain_store import Chain
from python_project.backbone.utils import (
    expand_ranges,
    GENESIS_DOT,
    GENESIS_LINK,
    Links,
    ranges,
    Ranges,
    wrap_return,
)

from tests.conftest import FakeBlock


class TestBatchInsert:
    num_blocks = 1000

    def test_insert(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(num_batches=1, num_blocks=self.num_blocks)
        last_blk = batches[0][-1]
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        wrap_return(insert_function(chain, batches[0]))
        assert len(chain.terminal) == 1
        assert last_blk_link in chain.terminal


class TestConflictsInsert:
    def test_two_conflict_seq_insert(
        self, insert_function, insert_function_copy, create_batches
    ):
        chain = Chain()
        batches = create_batches(num_batches=2, num_blocks=200)

        # Insert first batch sequentially
        last_blk = batches[0][-1]
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        wrap_return(insert_function(chain, batches[0]))
        assert len(chain.terminal) == 1
        assert last_blk_link in chain.terminal

        # Insert second batch sequentially
        last_blk = batches[1][-1]
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        wrap_return(insert_function_copy(chain, batches[1]))
        assert len(chain.terminal) == 2
        assert last_blk_link in chain.terminal

    @pytest.mark.parametrize("num_batches", [2 ** i for i in range(4, 10, 2)])
    def test_insert_many_conflicts(self, create_batches, num_batches, insert_function):
        chain = Chain()
        batches = create_batches(num_batches=num_batches, num_blocks=5)
        # Insert first batch sequentially
        i = 1
        for batch in batches:
            last_blk = batches[i - 1][-1]
            wrap_return(insert_function(chain, batch))
            last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
            assert len(chain.terminal) == i
            assert last_blk_link in chain.terminal
            i += 1


class TestFrontiers:
    def test_empty_frontier(self):
        chain = Chain()
        frontier = chain.frontier
        assert not frontier.holes
        assert not frontier.inconsistencies

        assert len(frontier.terminal) == 1
        assert frontier.terminal == GENESIS_LINK

    def test_insert_no_conflict(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 10)
        wrap_return(insert_function(chain, batches[0]))

        frontier = chain.frontier
        assert not frontier.holes
        assert not frontier.inconsistencies

        assert len(frontier.terminal) == 1
        assert all(10 in term for term in frontier.terminal)

    def test_insert_with_one_hole(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 10)

        # insert first half
        wrap_return(insert_function(chain, batches[0][:4]))
        # Skip one block
        wrap_return(insert_function(chain, batches[0][5:]))

        front = chain.frontier
        assert len(front.holes) == 1
        assert all(h == (5, 5) for h in front.holes)

        assert len(front.inconsistencies) == 1
        assert all(i[0] == 5 for i in front.inconsistencies)

        assert front.terminal[0][0] == 4 and front.terminal[1][0] == 10

    def test_insert_seq_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 100)

        # insert first half
        wrap_return(insert_function(chain, batches[0][:10]))
        # Skip 10 blocks
        wrap_return(insert_function(chain, batches[0][20:]))

        front = chain.frontier
        assert len(front.holes) == 1
        assert all(h == (11, 20) for h in front.holes)

        assert len(front.inconsistencies) == 1
        assert all(i[0] == 20 for i in front.inconsistencies)

        assert len(front.terminal) == 2
        assert front.terminal[0][0] == 10 and front.terminal[1][0] == 100

    def test_insert_multi_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 100)

        # insert first half
        wrap_return(insert_function(chain, batches[0][:10]))
        # Skip 5 blocks
        wrap_return(insert_function(chain, batches[0][15:50]))
        # Skip more 5 blocks
        wrap_return(insert_function(chain, batches[0][55:]))

        front = chain.frontier
        assert len(front.holes) == 2
        assert front.holes[0] == (11, 15) and front.holes[1] == (51, 55)

        assert len(front.inconsistencies) == 2
        assert front.inconsistencies[0][0] == 15 and front.inconsistencies[1][0] == 55

        assert len(front.terminal) == 3
        assert (
            front.terminal[0][0] == 10
            and front.terminal[1][0] == 50
            and front.terminal[2][0] == 100
        )

    def test_insert_conflicts_no_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert both batches
        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain, batches[1]))

        front = chain.frontier
        assert not front.holes
        assert not front.inconsistencies
        assert len(front.terminal) == 2
        assert front.terminal[0][0] == 100 and front.terminal[1][0] == 100

    def test_insert_conflicts_with_inconsistency(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert both batches
        wrap_return(insert_function(chain, batches[0]))
        # insert second half of the batch
        wrap_return(insert_function(chain, batches[1][50:]))

        front = chain.frontier
        assert not front.holes
        assert len(front.inconsistencies) == 1
        assert front.inconsistencies[0][0] == 50

        assert len(front.terminal) == 2
        assert front.terminal[0][0] == 100 and front.terminal[1][0] == 100

    def test_insert_conflicts_many_inconsistencies(
        self, create_batches, insert_function
    ):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert first with  batch
        wrap_return(insert_function(chain, batches[0]))
        # insert second batch with holes
        incons = ((5, 10), (30, 40), (50, 60), (80, 100))

        for ran in incons:
            wrap_return(insert_function(chain, batches[1][ran[0] : ran[1]]))

        front = chain.frontier
        assert not front.holes

        assert len(front.inconsistencies) == len(incons)
        assert all(
            front.inconsistencies[k][0] == incons[k][0] for k in range(len(incons))
        )

        assert len(front.terminal) == len(incons) + 1
        assert all(front.terminal[k][0] == incons[k][1] for k in range(len(incons)))


class TestFrontierReconciliation:
    def test_no_updates_no_conflicts(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(1, 100)

        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain2, batches[0]))

        front_diff = chain.reconcile(chain2.frontier)

        assert not front_diff.conflicts
        assert not front_diff.missing

    def test_chain_missing(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(1, 100)

        wrap_return(insert_function(chain, batches[0][:90]))
        wrap_return(insert_function(chain2, batches[0]))

        front_diff = chain.reconcile(chain2.frontier)

        assert not front_diff.conflicts
        assert all(k == (91, 100) for k in front_diff.missing)

    def test_chain_conflicting(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(2, 100)

        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain2, batches[1]))

        front_diff = chain.reconcile(chain2.frontier)

        assert len(front_diff.conflicts) == 1
        assert all(k[0] == 100 for k in front_diff.conflicts)
        assert not front_diff.missing

    def test_multiple_conflicts(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(3, 100)

        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain2, batches[1]))
        wrap_return(insert_function(chain2, batches[2]))

        front_diff = chain.reconcile(chain2.frontier)

        assert len(front_diff.conflicts) == 2
        assert all(k[0] == 100 for k in front_diff.conflicts)
        assert not front_diff.missing

    def test_past_conflict(self, create_batches, insert_function):
        chain = Chain(max_extra_dots=2)
        chain2 = Chain()
        batches = create_batches(2, 50)

        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain2, batches[0]))
        front_diff = chain.reconcile(chain2.frontier)
        assert not front_diff.missing
        assert not front_diff.conflicts

        # last reconcile point becomes 50

        wrap_return(insert_function(chain2, batches[1][:10]))
        front_diff = chain.reconcile(chain2.frontier, 50)

        assert not front_diff.missing
        # One conflict found
        assert len(front_diff.conflicts) == 1
        conf_dot = list(front_diff.conflicts)[0]
        assert len(front_diff.conflicts[conf_dot]) <= chain.max_extra_dots


class TestNextLinkIterator:
    def test_empty_iterator(self):
        chain = Chain()
        assert chain.get_next_links(GENESIS_DOT) is None

    def test_one_chain_iterator(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 10)[0]
        wrap_return(insert_function(chain, batches))
        val = chain.get_next_links(GENESIS_DOT)
        assert len(val) == 1
        assert val[0][0] == 1

    def test_next_terminal(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 10)
        for i in range(len(batches[0])):
            wrap_return(insert_function(chain, [batches[0][i]]))
            assert chain.get_next_links(chain.terminal[0]) is None

    def test_prev_iter(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 10)
        prev_links = Links((GENESIS_DOT,))
        for i in range(len(batches[0])):
            wrap_return(insert_function(chain, [batches[0][i]]))

            assert len(chain.get_prev_links(chain.terminal[0])) == 1
            assert chain.get_prev_links(chain.terminal[0]) == prev_links

            prev_links = chain.terminal


class TestConsistentTerminal:
    def test_empty_terminal(self):
        chain = Chain()
        assert chain.consistent_terminal == Links((GENESIS_DOT,))

    def test_linear_consistency(self, create_batches, insert_function):
        batches = create_batches(1, 10)
        chain = Chain()
        # insert first 10
        wrap_return(insert_function(chain, batches[0]))

        assert len(chain.consistent_terminal) == 1
        assert chain.consistent_terminal == chain.frontier.terminal

    def test_iterative_consistency(self, create_batches, insert_function):
        batches = create_batches(1, 10)
        chain = Chain()
        # insert first 10
        for i in range(10):
            wrap_return(insert_function(chain, [batches[0][i]]))
            assert len(chain.consistent_terminal) == 1
            assert chain.consistent_terminal[0][0] == i + 1

    def test_missing_blocks(self, create_batches, insert_function):
        batches = create_batches(1, 100)
        chain = Chain()

        incons = ((0, 10), (30, 40), (50, 60), (80, 100))
        holes = ranges({i for i in range(100)} - expand_ranges(Ranges(incons)))

        for ran in incons:
            wrap_return(insert_function(chain, batches[0][ran[0] : ran[1]]))
            assert len(chain.consistent_terminal) == 1
            assert chain.consistent_terminal[0][0] == 10

        # Fill up the holes
        for i in range(len(holes)):
            ran = holes[i]
            wrap_return(insert_function(chain, batches[0][ran[0] - 1 : ran[1] + 1]))
            assert len(chain.consistent_terminal) == 1
            assert chain.consistent_terminal[0][0] == incons[i + 1][1]

    def test_multi_version(self, create_batches, insert_function):
        batches = create_batches(2, 100)
        chain = Chain()
        wrap_return(insert_function(chain, batches[0]))
        wrap_return(insert_function(chain, batches[1]))

        assert len(chain.consistent_terminal) == 2
        assert (
            chain.consistent_terminal[0][0] == 100
            and chain.consistent_terminal[1][0] == 100
        )


class TestNewConsistentDots:
    def test_one_insert(self, create_batches):
        batchs = create_batches(1, 10)
        chain = Chain()
        blk = batchs[0][0]
        res = chain.add_block(blk.previous, blk.sequence_number, blk.hash)
        assert len(res) == 1
        assert res[0][0] == 1

    def test_iter_insert(self, create_batches):
        batchs = create_batches(1, 10)
        chain = Chain()

        for i in range(10):
            blk = batchs[0][i]
            res = chain.add_block(blk.links, blk.com_seq_num, blk.hash)
            assert len(res) == 1
            assert res[0][0] == i + 1

    def test_batch_insert(self, create_batches, insert_function):
        batchs = create_batches(1, 20)
        chain_obj = Chain()

        vals = wrap_return(insert_function(chain_obj, batchs[0][:10]))
        print(vals)
        print(chain(*vals))
        assert len(vals) == 10
        assert min(vals)[0] == 1 and max(vals)[0] == 10

        vals = wrap_return(insert_function(chain_obj, batchs[0][10:]))
        assert len(vals) == 10
        assert min(vals)[0] == 11 and max(vals)[0] == 20

    def test_insert_multi_forks(self, create_batches, insert_function):
        batchs = create_batches(2, 20)
        chain = Chain()

        vals = wrap_return(insert_function(chain, batchs[0][:10]))
        assert len(vals) == 10
        assert min(vals)[0] == 1 and max(vals)[0] == 10

        vals = wrap_return(insert_function(chain, batchs[1][:6]))
        assert len(vals) == 6
        assert min(vals)[0] == 1 and max(vals)[0] == 6

        vals = wrap_return(insert_function(chain, batchs[0][10:]))
        assert len(vals) == 10
        assert min(vals)[0] == 11 and max(vals)[0] == 20

        vals = wrap_return(insert_function(chain, batchs[1][10:]))
        assert len(vals) == 0

        vals = wrap_return(insert_function(chain, batchs[1][6:10]))
        assert len(vals) == 10 + 4
        assert min(vals)[0] == 7 and max(vals)[0] == 20

        for i in range(1, 21):
            print(chain.get_all_short_hash_by_seq_num(i))
            assert len(list(chain.get_dots_by_seq_num(i))) == 2

    def test_insert_with_merge_block(self, create_batches, insert_function):
        batches = create_batches(2, 10)
        chain = Chain()

        last_blk1 = batches[0][-1]
        last_blk2 = batches[1][-1]

        dot1 = (last_blk1.com_seq_num, last_blk1.short_hash)
        dot2 = (last_blk2.com_seq_num, last_blk2.short_hash)

        vals = wrap_return(insert_function(chain, batches[0]))
        assert len(vals) == 10
        assert vals[0][0] == 1 and vals[-1][0] == 10

        merge_block = FakeBlock(links=Links((dot1, dot2)))
        chain.add_block(merge_block.links, merge_block.com_seq_num, merge_block.hash)

        vals = wrap_return(insert_function(chain, batches[1]))
        assert len(vals) == 11
        assert vals[0][0] == 1 and vals[-1][0] == 11

        assert len(list(chain.get_dots_by_seq_num(11))) == 1


def test_empty_get_dots(create_batches):
    chain = Chain()
    v = chain.get_dots_by_seq_num(1)
    assert len(list(v)) == 0
