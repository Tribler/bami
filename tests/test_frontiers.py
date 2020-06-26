import pytest
from python_project.backbone.datastore.frontiers import Frontier
from python_project.backbone.datastore.utils import Links, Ranges, encode_raw


class StdVals:
    terminal = Links(((5, "test1"),))
    term_bits = (True,)
    holes = Ranges(())
    incon = Links(())
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


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_consistency(val):
    f = Frontier(val.terminal, val.holes, val.incon, val.term_bits)
    assert f.consistent_terminal == val.con_terminal


@pytest.mark.parametrize("val", [StdVals, InconVals])
def test_frontier_bytes_convert(val):
    f = Frontier(val.terminal, val.holes, val.incon, val.term_bits)
    assert Frontier.from_bytes(f.to_bytes()) == f
