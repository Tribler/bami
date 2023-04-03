from bami.spar.rank import IncrementalPageRank


def test_rank():
    pr = IncrementalPageRank()

    pr.add_edge(0, 1, )
    pr.add_edge(0, 2, weight=0.5)
    pr.add_edge(1, 2, weight=2.0)

    # Initalize calculating rank from the standpoint of node "0"
    pr.calculate(0)

    # Get the score for node "1" from the standpoint of the node "0"
    print(pr.get_node_score(0, 1))

    # Add another edge: note that the scores are automatically recalculated
    pr.add_edge(2, 1, weight=3.0)
    print(pr.get_node_score(0, 1))