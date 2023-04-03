import random
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple, TypeAlias

import networkx as nx

# TODO: use __slots__ to speed up stuff:
# https://tommyseattle.com/tech/python-class-dict-named-tuple-memory-and-perf.html

NodeId: TypeAlias = int


def sign(x: float) -> int:
    if x > 0:
        return 1
    elif x < 0:
        return -1
    return 0


class NodeDoesNotExist(Exception):
    pass


class RandomWalk(List[NodeId]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uuid = uuid.uuid4()

    def calculate_penalties(self, neg_weights: Dict[NodeId, float]) -> (
            Dict[NodeId, float]):
        # Penalty calculation logic:
        # 1. The penalty is accumulated by walking backwards from the last
        # node in the segment.
        # 2. If a node is encountered in the walk more than once, its penalty
        # is updated to the highest current accumulated penalty
        # 3. If a penalty-inducing node (called a "neg" for short)
        # is encountered more than once, its effect is not accumulated.
        #
        # In a sense, every neg in the walk produces a "tag", so each node
        # in the walk leading up to a given neg is "tagged" by it,
        # and then each "tagged" node is penalized according
        # to the weight of the "tags" associated with the negs.
        #
        # Example:
        # nodes D and F both are negs of weight 1
        # node B is repeated twice in positions 2-3
        #         ◄─+tag F────────┐
        #         ◄─+tag D──┐     │
        #        ┌──────────┴─────┴────┐
        #        │ A  B  B  D  E  F  G │
        #        └─────────────────────┘
        # Resulting penalties for the nodes:
        # node     A  -  B  D  E  F  G
        # "tags"   DF -  DF F  F
        # penalty  2  -  2  2  1  1  0
        #
        penalties: Dict[NodeId, float] = {}
        negs = neg_weights.copy()
        accumulated_penalty = 0.0
        for step in reversed(self):
            if penalty := negs.pop(step, None):
                accumulated_penalty += penalty
            if accumulated_penalty != 0.0:
                penalties[step] = accumulated_penalty
        return penalties


@dataclass
class PosWalk:
    __slots__ = ['pos', 'walk']
    pos: int
    walk: RandomWalk


class WalkStorage:

    def __init__(self) -> None:
        self.__walks: Dict[NodeId, Dict[uuid.UUID, PosWalk]] = {}

    def walks_starting_from_node(self, src: NodeId) -> list[RandomWalk]:
        """ Returns the walks starting from the given node"""
        return [pos_walk.walk for pos_walk in
                self.__walks.get(src, {}).values() if pos_walk.pos == 0]

    def add_walk(self, walk: RandomWalk, start_pos: int = 0):
        for pos, node in enumerate(walk):
            if pos < start_pos:
                continue
            # Get existing walks for the node. If there is none,
            # create an empty dictionary for those
            walks_with_node = self.__walks[node] = self.__walks.get(node, {})

            # Avoid storing the same walk twice in case it visits the node
            # more than once. We're only interested in the first visit,
            # because in the case of an incremental update, the part of the
            # walk that occurs after just the first visit to the node
            # becomes invalid.
            if walk.uuid in walks_with_node:
                continue

            # To prevent linear scanning, we store the node's position in the walk
            walks_with_node[walk.uuid] = PosWalk(pos, walk)

    def drop_walks_from_node(self, node: NodeId):
        for walk in self.walks_starting_from_node(node):
            for affected_node in walk:
                del self.__walks[affected_node][walk.uuid]
        self.__walks.get(node, {}).clear()

    def get_walks_through_node(self, node: NodeId):
        return self.__walks.get(node, {})

    def invalidate_walks_through_node(self, invalidated_node: NodeId) -> \
            List[Tuple[RandomWalk, RandomWalk]]:
        if (walks := self.__walks.get(invalidated_node)) is None:
            return []
        invalidated_walks = []
        for uid, pos_walk in walks.items():
            pos, walk = pos_walk.pos, pos_walk.walk
            # For every node mentioned in the invalidated subsequence,
            # remove the corresponding entries in the bookkeeping dict
            invalidated_segment = RandomWalk(walk[pos + 1:])
            for affected_node in invalidated_segment:
                # if the invalidated node is encountered in the invalidated
                # subsequence (i.e. there was more than one copy in the
                # original walk) - don't accidentally remove it, for not to
                # shoot ourselves in the foot (it still belongs to the
                # remaining segment).
                if affected_node == invalidated_node:
                    continue
                self.__walks.get(affected_node, {}).pop(uid, None)

            # Remove the invalidated subsequence from the walk
            del walk[pos + 1:]
            invalidated_walks.append((walk, invalidated_segment))
        return invalidated_walks

    def get_walks(self, node):
        return self.__walks.get(node)


class IncrementalPageRank:
    def __init__(self, graph=None, persistent_storage=None) -> None:
        self.__persistent_storage = persistent_storage
        # FIXME: graph vs persistent_storage options
        rank_calc_commands = None
        if self.__persistent_storage is not None:
            graph, rank_calc_commands = self.__persistent_storage.get_graph_and_calc_commands()

        self.__graph = nx.DiGraph(graph)
        self.__walks = WalkStorage()
        self.__personal_hits: Dict[NodeId, Counter] = {}
        self.__neg_hits: Dict[NodeId, Dict[NodeId, float]] = {}
        self.alpha = 0.85

        if rank_calc_commands is not None:
            for node, num_walks in rank_calc_commands.items():
                self.calculate(node, num_walks)

    def get_walks_count_for_node(self, src: NodeId):
        return len(self.__walks.walks_starting_from_node(src))

    def calculate(self, ego: NodeId, num_walks: int = 1000):
        """
        Calculate the PageRank from the perspective of the given node.
        If there are already walks for the node, drop them and calculate anew.
        :param ego: The source node to calculate the PageRank for.
        :param num_walks: The number of walks that should be used
        """
        if self.__persistent_storage is not None:
            self.__persistent_storage.put_rank_calc_command(ego, num_walks)
        self.__walks.drop_walks_from_node(ego)

        if not self.__graph.has_node(ego):
            raise NodeDoesNotExist

        negs = self.__neighbours_weighted(ego, positive=False)

        counter = self.__personal_hits[ego] = Counter()
        for _ in range(0, num_walks):
            walk = self.perform_walk(ego)
            counter.update(walk[1:])
            self.__walks.add_walk(walk)
            self.__update_negative_hits(walk, negs)

    def get_node_score(self, ego: NodeId, target: NodeId):
        counter = self.__personal_hits[ego]
        # TODO: optimize by caching the result?
        assert ego not in counter
        hits = counter[target]

        # TODO: normalize the negative hits?
        hits_penalized = hits + self.__neg_hits.get(ego, {}).get(target, 0)
        return hits_penalized / counter.total()

    def get_ranks(self, ego: NodeId, limit=None):
        # FIXME: optimize out repeated totals, etc.
        counter = self.__personal_hits[ego]
        peer_scores = []
        for peer in counter.keys():
            peer_scores.append((peer, self.get_node_score(ego, peer)))

        sorted_ranks = sorted(peer_scores, key=lambda x: x[1], reverse=True)[
                       :limit]

        return dict(sorted_ranks)

    def get_ordered_peers(self, ego: NodeId, limit=None):
        return [k for k, v in sorted(self.get_ranks(ego, limit).items(),
                                     key=lambda x: x[1], reverse=True)]

    def perform_walk(self, start_node: NodeId) -> RandomWalk:
        walk = RandomWalk([start_node])
        new_segment = self.__generate_walk_segment(
            start_node, stop_node=start_node)
        walk.extend(new_segment)
        return walk

    def __neighbours_weighted(self, node: NodeId, positive=True) -> Dict[
        NodeId, float]:
        neighbours = {}
        for nbr in self.__graph.neighbors(node):
            # Only return positive/negative neighbours
            weight = self.__graph[node][nbr]['weight']
            if weight == 0:
                continue
            if positive and weight > 0 or not positive and weight < 0:
                neighbours[nbr] = weight
        return neighbours

    def __generate_walk_segment(self, start_node: NodeId,
                                stop_node: NodeId) -> RandomWalk:
        node = start_node
        walk = RandomWalk()
        while ((neighbours := self.__neighbours_weighted(node))
               and random.random() <= self.alpha):
            peers, weights = zip(*neighbours.items())
            next_step = random.choices(peers, weights=weights, k=1)[0]
            if next_step == stop_node:
                break
            walk.append(next_step)
            node = next_step
        return walk

    def get_edge(self, src: NodeId, dest: NodeId) -> float | None:
        if not self.__graph.has_edge(src, dest):
            return None
        return self.__graph[src][dest]['weight']

    def get_node_edges(self, node: NodeId) -> list[tuple[NodeId, NodeId, float]] | None:
        if not self.__graph.has_node(node):
            return None
        return list(self.__graph.edges(node, data='weight'))

    def __persist_edge(self, src: NodeId, dest: NodeId, weight: float = 1.0):
        if self.__persistent_storage is not None:
            if weight == 0.0:
                self.__persistent_storage.remove_edge(src, dest)
            else:
                self.__persistent_storage.put_edge(src, dest, weight)

    def __update_penalties_for_edge(self,
                                    src: NodeId,
                                    dest: NodeId,
                                    remove_penalties: bool = False) -> None:
        # Note: there are two ways to iterate over this:
        # 1. first get all the walks through the new destination node,
        #   then filter the results for walks that start with the source (ego) node.
        # 2. first get all the walks starting from the source (ego) node, then
        #   filter the results for walks that pass through the destination node.
        # The method (1) should be much more efficient for all cases but very specific
        # ones, such as extra-popular nodes in databases with a very large number of
        # ego nodes (i.e. maintained ratings). The reasons for efficiency of (1) are
        # a. for each ego node there are always at least 1k walks
        # b. we would have to sequentially iterate through all the steps in every
        #   single one of those 1k walks.
        # Therefore, we use method (1)

        # Get all walks that pass through the destination node and start with the source node
        affected_walks = []
        walks_through_dest = self.__walks.get_walks_through_node(dest)
        for pw in walks_through_dest.values():
            if pw.walk[0] == src:
                affected_walks.append(pw.walk)

        # Calculate penalties and update neg_hits for each affected walk
        weight = self.__graph[src][dest]['weight']
        ego_neg_hits = self.__neg_hits.setdefault(src, {})
        for walk in affected_walks:
            penalties = walk.calculate_penalties({dest: weight})
            for node, penalty in penalties.items():
                if remove_penalties:
                    penalty = -penalty
                ego_neg_hits[node] = ego_neg_hits.setdefault(node, 0) + penalty

    def __recalc_invalidated_walk(self, walk, invalidated_segment):
        # Possible optimization: instead of dropping the walk and then
        # recalculating it, reuse fragments from previously dropped walks.
        # The problem is care must be taken to handle loops when going through
        # the changed edges, and fair stochastic selection of fragments.

        # Subtract the nodes in the invalidated sequence from the hit
        # counter for the starting node of the invalidated walk.
        ego = walk[0]
        counter = self.__personal_hits[ego]
        counter.subtract(invalidated_segment)

        # Finish the invalidated walk. The stochastic nature of random
        # walks allows us to complete a walk by just continuing it until
        # it stops naturally.
        new_segment_start = len(walk)
        new_segment = self.__generate_walk_segment(walk[-1], stop_node=ego)
        walk.extend(new_segment)

        counter.update(walk[new_segment_start:])
        self.__walks.add_walk(walk, start_pos=new_segment_start)

    def add_edge(self, src: NodeId, dest: NodeId, weight: float = 1.0):
        old_edge = self.get_edge(src, dest)
        old_weight = self.__graph[src][dest]['weight'] if old_edge else 0.0
        if old_weight == weight:
            return
        self.__persist_edge(src, dest, weight)

        # There are nine cases for adding/updating an edge: the variants are
        # negative, zero, and positive weight for both the old and the new
        # state of the edge. For clarity, we arrange all the variants in a
        # matrix, and then call the necessary variant. The functions are
        # coded by the combination of the old and new weights of the edge, e.g:
        # "zp" = zero old weight -> positive new weight, etc.

        def zz(*_):
            # Nothing to do - noop
            pass

        def zp(s, d, w):
            # Clear penalties resulting from the invalidated walks
            invalidated_walks = self.__walks.invalidate_walks_through_node(s)

            negs_cache = {}
            for (walk, invalidated_segment) in invalidated_walks:
                negs = negs_cache[walk[0]] = self.__neighbours_weighted(
                    walk[0], positive=False)
                # The change includes a positive edge (former or new),
                # so we must first clear the negative walks going through it.
                self.__update_negative_hits(
                    RandomWalk(walk + invalidated_segment), negs,
                    subtract=True)

            if w == 0.0:
                if self.__graph.has_edge(s, d):
                    self.__graph.remove_edge(s, d)
            else:
                self.__graph.add_edge(s, d, weight=w)

            # Restore the walks and recalculate the penalties
            for (walk, invalidated_segment) in invalidated_walks:
                self.__recalc_invalidated_walk(walk, invalidated_segment)
                self.__update_negative_hits(walk, negs_cache[walk[0]])

        def zn(s, d, w):
            self.__graph.add_edge(s, d, weight=w)
            self.__update_penalties_for_edge(s, d)

        def pz(s, d, _):
            zp(s, d, 0.0)

        def pp(*args):
            zp(*args)

        def pn(*args):
            pz(*args)
            zn(*args)

        def nz(s, d, _):
            # The old edge was a negative edge, so we must clear all
            # the negative walks produced by it (i.e. ended in it) for
            # the respective ego node. Effectively, this means removing
            # all the negative walks starting from src and ending in dest.
            self.__update_penalties_for_edge(s, d, remove_penalties=True)
            self.__graph.remove_edge(s, d)

        def np(*args):
            nz(*args)
            zp(*args)

        def nn(*args):
            nz(*args)
            zn(*args)

        row = sign(old_weight)
        column = sign(weight)
        [[zz, zp, zn],
         [pz, pp, pn],
         [nz, np, nn]][row][column](src, dest, weight)

    def __update_negative_hits(self,
                               walk: RandomWalk,
                               negs: Dict[NodeId, float],
                               subtract: bool = False) -> None:
        if not set(negs).intersection(walk):
            return
        ego_neg_hits = self.__neg_hits.setdefault(walk[0], {})

        # Note this can be further optimized by memorizing the positions of
        # the negs during the walk generation.

        for node, penalty in walk.calculate_penalties(negs).items():
            if subtract:
                penalty = -penalty
            ego_neg_hits[node] = ego_neg_hits.get(node, 0) + penalty