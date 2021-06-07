from copy import copy

import pytest
from bami.plexus.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.plexus.backbone.utils import Links, Ranges


class StdVals:
    terminal = Links(((5, b"test1"),))
    term_bits = (True,)
    holes = Ranges(())
    incon = Links(())
    conflicts = {}
    con_terminal = terminal


class InconVals:
    terminal = Links(((0, b"gen"), (5, b"test1")))
    term_bits = (
        True,
        False,
    )
    holes = Ranges(((1, 4),))
    incon = Links(((4, b"test1"),))
    con_terminal = Links(((0, b"gen"),))
    conflicts = {(5, b"test1"): {1: (b"gen",), 5: (b"test2",)}}


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_bytes_convert(val):
    f = Frontier(val.terminal, val.holes, val.incon)
    assert Frontier.from_bytes(f.to_bytes()) == f


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_diff_bytes_convert(val):
    f = FrontierDiff(val.holes, val.conflicts)
    assert FrontierDiff.from_bytes(f.to_bytes()) == f


def test_compare_frontiers():
    val = StdVals
    f = Frontier(val.terminal, val.holes, val.incon)
    f2 = copy(f)
    f2.terminal = Links(((6, b"test2"),))

    assert f != f2
    assert f2 > f

    inc_f = Frontier(InconVals.terminal, InconVals.holes, InconVals.incon)
    assert f > inc_f
    assert f2 > inc_f
