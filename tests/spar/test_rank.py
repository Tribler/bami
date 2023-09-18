import networkx as nx
import numpy as np

from bami.spar.rank import IncrementalMeritRank


def test_rank():
    pr = IncrementalMeritRank()

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


def test_benchmark():
    # Generate a random network with N = 7000 nodes with weights in the range [0, 1]
    G = nx.gnp_random_graph(7000, 0.0005, directed=True)
    for (u, v) in G.edges():
        G.edges[u, v]['weight'] = np.random.rand()

    # Run IncrementalMeritRank on the network and time it
    # time it for 10 iterations



    pr = IncrementalMeritRank()
    pr.calculate(0)
    vals = pr.get_ranks(0)




