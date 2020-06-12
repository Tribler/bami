import pytest
from python_project.backbone.datastore.chain_store import Chain


class TestBatchInsert:
    num_blocks = 1000

    def test_insert(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(num_batches=1, num_blocks=self.num_blocks)
        insert_function(chain, batches[0])
        assert len(chain.terminal) == 1


class TestConflictsInsert:

    def test_two_conflict_seq_insert(self, insert_function, insert_function_copy, create_batches):
        chain = Chain()
        batches = create_batches(num_batches=2, num_blocks=200)

        # Insert first batch sequentially
        last_blk = batches[0][-1]
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        insert_function(chain, batches[0])
        assert len(chain.terminal) == 1
        assert last_blk_link in chain.terminal

        # Insert second batch sequentially
        last_blk = batches[1][-1]
        last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
        insert_function_copy(chain, batches[1])
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
            insert_function(chain, batch)
            last_blk_link = (last_blk.com_seq_num, last_blk.short_hash)
            assert len(chain.terminal) == i
            assert last_blk_link in chain.terminal
            i += 1


class TestFrontiers:

    def test_empty_frontier(self, genesis_link):
        chain = Chain()
        frontier = chain.frontier
        assert not frontier.holes
        assert not frontier.inconsistencies

        assert len(frontier.terminal) == 1
        assert frontier.terminal == genesis_link

    def test_insert_no_conflict(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 10)
        insert_function(chain, batches[0])

        frontier = chain.frontier
        assert not frontier.holes
        assert not frontier.inconsistencies

        assert len(frontier.terminal) == 1
        assert all(10 in term for term in frontier.terminal)

    def test_insert_with_one_hole(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 10)

        # insert first half
        insert_function(chain, batches[0][:4])
        # Skip one block
        insert_function(chain, batches[0][5:])

        front = chain.frontier
        assert all(h == (5, 5) for h in front.holes)
        assert not front.inconsistencies
        assert front.terminal[0][0] == 4 and front.terminal[1][0] == 10

    def test_insert_seq_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 100)

        # insert first half
        insert_function(chain, batches[0][:10])
        # Skip 10 blocks
        insert_function(chain, batches[0][20:])

        front = chain.frontier
        assert all(h == (11, 20) for h in front.holes)
        assert not front.inconsistencies
        assert front.terminal[0][0] == 10 and front.terminal[1][0] == 100

    def test_insert_multi_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(1, 100)

        # insert first half
        insert_function(chain, batches[0][:10])
        # Skip 5 blocks
        insert_function(chain, batches[0][15:50])
        # Skip more 5 blocks
        insert_function(chain, batches[0][55:])

        front = chain.frontier
        assert len(front.holes) == 2
        assert front.holes[0] == (11, 15) and front.holes[1] == (51, 55)
        assert not front.inconsistencies
        assert len(front.terminal) == 3
        assert front.terminal[0][0] == 10 and front.terminal[1][0] == 50 and front.terminal[2][0] == 100

    def test_insert_conflicts_no_holes(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert both batches
        insert_function(chain, batches[0])
        insert_function(chain, batches[1])

        front = chain.frontier
        assert not front.holes
        assert not front.inconsistencies
        assert len(front.terminal) == 2
        assert front.terminal[0][0] == 100 and front.terminal[1][0] == 100

    def test_insert_conflicts_with_inconsistency(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert both batches
        insert_function(chain, batches[0])
        # insert second half of the batch
        insert_function(chain, batches[1][50:])

        front = chain.frontier
        assert not front.holes
        assert len(front.inconsistencies) == 1
        assert front.inconsistencies[0][0] == 50

        assert len(front.terminal) == 2
        assert front.terminal[0][0] == 100 and front.terminal[1][0] == 100

    def test_insert_conflicts_many_inconsistencies(self, create_batches, insert_function):
        chain = Chain()
        batches = create_batches(2, 100)

        # insert first with  batch
        insert_function(chain, batches[0])
        # insert second batch with holes
        incons = (
            (5, 10), (30, 40),
            (50, 60), (80, 100)
        )

        for ran in incons:
            insert_function(chain, batches[1][ran[0]:ran[1]])

        front = chain.frontier
        assert not front.holes

        assert len(front.inconsistencies) == len(incons)
        assert all(front.inconsistencies[k][0] == incons[k][0] for k in range(len(incons)))

        assert len(front.terminal) == len(incons) + 1
        assert all(front.terminal[k][0] == incons[k][1] for k in range(len(incons)))


class TestFrontierReconciliation:

    def test_no_updates_no_conflicts(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(1, 100)

        insert_function(chain, batches[0])
        insert_function(chain2, batches[0])

        front_diff = chain.reconcile(chain2.frontier)

        assert not front_diff.conflicts
        assert not front_diff.missing

    def test_chain_missing(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(1, 100)

        insert_function(chain, batches[0][:90])
        insert_function(chain2, batches[0])

        front_diff = chain.reconcile(chain2.frontier)

        assert not front_diff.conflicts
        assert all(k == (91, 100) for k in front_diff.missing)

    def test_chain_conflicting(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(2, 100)

        insert_function(chain, batches[0])
        insert_function(chain2, batches[1])

        front_diff = chain.reconcile(chain2.frontier)

        assert len(front_diff.conflicts) == 1
        assert all(k[0] == 100 for k in front_diff.conflicts)
        assert not front_diff.missing

    def test_multiple_conflicts(self, create_batches, insert_function):
        chain = Chain()
        chain2 = Chain()
        batches = create_batches(3, 100)

        insert_function(chain, batches[0])
        insert_function(chain2, batches[1])
        insert_function(chain2, batches[2])

        front_diff = chain.reconcile(chain2.frontier)

        assert len(front_diff.conflicts) == 2
        assert all(k[0] == 100 for k in front_diff.conflicts)
        assert not front_diff.missing
