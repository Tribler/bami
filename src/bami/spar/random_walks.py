import networkx as nx
import numpy as np

from collections import Counter, defaultdict
import random
from typing import Callable, Optional, Dict, List

from enum import Enum


class BiasStrategies(Enum):
    EDGE_WEIGHT = 1
    NO_WEIGHT = 2


class RandomWalks:

    def __init__(self, graph: nx.DiGraph, alpha: float = 1.0, base_number_random_walks: int = 1000) -> None:
        """Calculate the random walks for a given graph.
        @param graph: The graph to calculate the random walks for.
        @param alpha: Score value used for the bias.
        @param base_number_random_walks: The number of random walks to run.
        """
        self.alpha = alpha
        self.graph = graph

        self.random_walks = {}
        self.seed_node = None
        self.counters = {}
        self.neg_counters = {}

        self.num_edge_walks = defaultdict(int)
        self.hits = {}
        self.neg_hits = {}

        self.number_of_walks = {}
        self.base_number_of_random_walks = base_number_random_walks

        self.coleadings = defaultdict(lambda: defaultdict(int))

        self.penalties = defaultdict(int)

    def weight(self, current_node: int, neigh: int, cur_weight: float = None, revert: bool = False) -> float:
        """Score function for random walk.
        score = min(weight(u,v), cur_weight).
        @param neigh:
        @type neigh:
        @param current_node:
        @type current_node:
        @param cur_weight: The current weight to compare against. If none, the weight is set to infinity.
        @param revert: If true, make a reverse walk (default: False).
        @return: The score."""
        cur_weight = float('inf') if not cur_weight else cur_weight
        if revert:
            current_node, neigh = neigh, current_node
        return min(abs(self.graph.get_edge_data(current_node, neigh, default={'weight': 0})['weight']), cur_weight)

    def no_weight(self, current_node: int, neigh: int, cur_weight: float = None, revert: bool = False) -> float:
        """Score function for random walk.
        score = 1.
        @param u: The current node.
        @param v: The target neighbor node.
        @param cur_weight: The current weight to compare against. If none, the weight is set to infinity.
        @param revert: If true, make a reverse walk (default: False).
        @return: The score."""
        return 1.0

    def neigh(self, x: int, back_random_walk: bool = False) -> List:
        try:
            return list(self.graph.predecessors(x)) if back_random_walk else list(self.graph.successors(x))
        except nx.NetworkXError as _:
            return []

    def _compute_random_walks(self,
                              seed_node: int,
                              weight_func: Callable[[int, int, Optional[float], Optional[bool]], float],
                              reset_probability: float,
                              back_random_walk: bool = False,
                              update_weight: bool = False
                              ) -> List:
        random_walk = [seed_node]
        cur_weight = float('inf')
        c = random.uniform(0, 1)

        curent_coleadings = defaultdict(lambda: set())

        while c > reset_probability and len(self.neigh(random_walk[-1], back_random_walk)) > 0:
            current_node = random_walk[-1]
            current_neighbors = self.neigh(current_node, back_random_walk)
            current_edge_weights = np.array(
                [weight_func(current_node, neighbor, cur_weight, back_random_walk) for neighbor in current_neighbors])
            cumulated_edge_weights = np.cumsum(current_edge_weights)

            if cumulated_edge_weights[-1] == 0:
                break

            random_id = list(
                cumulated_edge_weights < (random.uniform(0, 1) * cumulated_edge_weights[-1])).index(
                False)
            next_node = current_neighbors[random_id]
            if update_weight:
                cur_weight = min(current_edge_weights[random_id], cur_weight)
            self.num_edge_walks[(current_node, next_node)] += 1

            from_node = next_node if back_random_walk else current_node
            to_node = current_node if back_random_walk else next_node
            w = self.graph.get_edge_data(from_node, to_node, default={'weight': 0})['weight']
            if w < 0:
                self.neg_counters[seed_node][next_node] += 1
                self.neg_hits[seed_node][next_node] += 1
                break

            for prev_node in reversed(random_walk):
                if prev_node == seed_node or next_node == seed_node:
                    break
                curent_coleadings[next_node].add(prev_node)

            random_walk.append(next_node)
            c = random.uniform(0, 1)

        # Update global coleadings
        for k, s_set in curent_coleadings.items():
            for s in s_set:
                self.coleadings[k][s] += 1

        if seed_node not in self.random_walks:
            self.random_walks[seed_node] = []

        self.random_walks[seed_node].append(random_walk)
        return random_walk

    def run_one_walk(self,
                     seed_node: int,
                     reset_probability: float = 0.33,
                     back_random_walk: bool = False,
                     bias_strategy: BiasStrategies = BiasStrategies.EDGE_WEIGHT,
                     update_weight: bool = False,
                     penalties: Dict[int, int] = None) -> List:
        """Run a single random walk.
        @param seed_node: The seed node.
        @param reset_probability: The probability of resetting the random walk.
        @param back_random_walk: If true, make a reverse walk (default: False).
        @param bias_strategy: The strategy to use for the bias. (default: BiasStrategies.EDGE_WEIGHT)
        @param update_weight: If true, the weight bounds are used. (default: False)
        @param penalties: The penalties for each node. (default: None)
        @return: The list of nodes in the random walk.
        """
        w_func = None
        if bias_strategy == BiasStrategies.EDGE_WEIGHT:
            w_func = self.weight
        elif bias_strategy == BiasStrategies.NO_WEIGHT:
            w_func = self.no_weight

        if penalties:
            self.penalties = penalties

        return self._compute_random_walks(seed_node, w_func,
                                          reset_probability, back_random_walk,
                                          update_weight)

    def run_with_all_negative_walks(self,
                                    seed_node: int,
                                    prw: int = 1000,
                                    prp: float = 0.15,
                                    nrw: int = 1000,
                                    nrp: float = 0.2
                                    ) -> None:
        # Positive walks
        self.run(seed_node, prw, prp,
                 False)
        # All negative walks
        for k in list(self.neg_hits[seed_node].keys()):
            self.run(k, nrw, nrp, back_random_walk=True)

    def run(self,
            seed_node: int,
            num_random_walks: int = 5000,
            reset_probability: float = 0.33,
            back_random_walk: bool = False,
            bias_strategy: BiasStrategies = BiasStrategies.EDGE_WEIGHT,
            update_weight: bool = False,
            penalties: Dict[int, int] = None
            ) -> None:
        """Run multiple random walks.
        @param seed_node: The seed node.
        @param num_random_walks: The number of random walks to run.
        @param reset_probability: The probability of resetting the random walk.
        @param back_random_walk: If true, make a reverse walk (default: False).
        @param bias_strategy: The strategy to use for the bias. (default: BiasStrategies.EDGE_WEIGHT)
        @param update_weight: If true, the weight bounds are used. (default: False)
        @param penalties: The penalties for each node. (default: None)
        """
        self.num_edge_walks = defaultdict(int)
        self.random_walks[seed_node] = []
        self.neg_hits[seed_node] = defaultdict(int)
        self.neg_counters[seed_node] = defaultdict(int)

        for n in range(num_random_walks):
            self.run_one_walk(seed_node, reset_probability,
                              back_random_walk, bias_strategy,
                              update_weight, penalties)

        self.number_of_walks[seed_node] = num_random_walks
        self.counters[seed_node] = Counter(x for xs in self.random_walks[seed_node] for x in xs)
        self.hits[seed_node] = Counter(x for xs in self.random_walks[seed_node] for x in set(xs))

    def has_node(self, node: int) -> bool:
        return node in self.random_walks

    def get_total_positive_hits(self, seed_node: int, target_node: int) -> float:
        """Get number of times the target node was visited in the run.
        @param seed_node: The seed node.
        @param target_node: The target node.
        @return: The number of times the target node was visited."""
        return self.counters[seed_node].get(target_node, 0)

    def get_total_negative_hits(self, seed_node: int, target_node: int) -> float:
        return self.neg_counters.get(seed_node, lambda: {}).get(target_node, 0)

    def get_total_positive_walk_hits_sum(self, seed_node: int) -> float:
        return sum(self.counters[seed_node].values())

    def get_total_negative_walk_hits_sum(self, seed_node: int) -> float:
        return sum(self.neg_counters[seed_node].values())

    def get_total_negative_nodes(self, seed_node: int) -> List[int]:
        return list(self.neg_counters[seed_node].keys())

    def get_number_positive_hits(self, seed_node: int, target_node: int) -> float:
        """Get number of walks that hit the target node.
        @param seed_node: The seed node.
        @param target_node: The target node.
        @return: Number of hits of target node."""
        if seed_node == target_node:
            return self.number_of_walks[seed_node]
        return self.hits[seed_node].get(target_node, 0)

    def get_number_negative_hits(self, seed_node: int, target_node: int) -> float:
        """Get number of walks that hit the target node.
        @param seed_node: The seed node.
        @param target_node: The target node.
        @return: Number of hits of target node."""
        return self.neg_hits.get(seed_node, lambda: {}).get(target_node, 0)

    def get_number_negative_nodes(self, seed_node: int) -> List[int]:
        return list(self.neg_hits[seed_node].keys())

    def get_number_positive_hits_sum(self, seed_node: int) -> float:
        return sum(self.hits[seed_node].values())

    def get_number_negative_hits_sum(self, seed_node: int) -> float:
        return sum(self.neg_hits[seed_node].values())
