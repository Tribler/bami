import pytest
from bami.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.backbone.utils import Links, Ranges


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
