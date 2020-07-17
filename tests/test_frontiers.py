import pytest
from python_project.backbone.datastore.frontiers import Frontier, FrontierDiff
from python_project.backbone.datastore.utils import Links, Ranges


class StdVals:
    terminal = Links(((5, "test1"),))
    term_bits = (True,)
    holes = Ranges(())
    incon = Links(())
    conflicts = {}
    con_terminal = terminal


class InconVals:
    terminal = Links(((0, "gen"), (5, "test1")))
    term_bits = (
        True,
        False,
    )
    holes = Ranges(((1, 4),))
    incon = Links(((4, "test1"),))
    con_terminal = Links(((0, "gen"),))
    conflicts = {(5, "test1"): {1: ("gen",), 5: ("test2",)}}


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_bytes_convert(val):
    f = Frontier(val.terminal, val.holes, val.incon)
    assert Frontier.from_bytes(f.to_bytes()) == f


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_diff_bytes_convert(val):
    f = FrontierDiff(val.holes, val.conflicts)
    assert FrontierDiff.from_bytes(f.to_bytes()) == f
