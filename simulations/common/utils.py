import time
from copy import deepcopy
from functools import reduce, wraps
from itertools import product
from operator import getitem

import networkx as nx


def time_mark(func):
    @wraps(func)
    def timeit_wrapper(self, *args, **kwargs):
        start_time = time.perf_counter()
        self._start_time = start_time
        result = func(self, *args, **kwargs)
        return result

    return timeit_wrapper


def set_time_mark(self):
    self._start_time = time.perf_counter()


def set_nested_value(dd, *keys):
    """Set item in nested dictionary
    :param dd: nested dictionary
    :param keys: keys to set. The last key is the value to set
    :return: nested dictionary
    """
    val = keys[-1]
    mapList = keys[:-1]
    reduce(getitem, mapList[:-1], dd)[mapList[-1]] = val
    return dd


def get_nested_value(dd, *keys):
    """Get item in nested dictionary"""
    for key in keys:
        try:
            dd = dd[key]
        except KeyError:
            return None
    if not (dd):
        return None
    return dd


def make_symmetric(matrix):
    vals = matrix.keys()
    comb = product(vals, vals)

    for c in comb:
        if c[0] not in matrix or c[1] not in matrix[c[0]]:
            matrix[c[0]][c[1]] = matrix[c[1]][c[0]]


def to_hash(str_msg):
    return str(hex(abs(hash(str_msg))))


def random_topology(num_peers=25, d=8):
    # Create network topology
    G = nx.random_regular_graph(d=8, n=num_peers)
    nx.relabel_nodes(G, {k: k + 1 for k in G.nodes()}, copy=False)
    return G


def connected_topology(num_peers):
    G = nx.complete_graph(num_peers)
    nx.relabel_nodes(G, {k: k + 1 for k in G.nodes()}, copy=False)
    return G


class Cache:
    """Utility class to work with Dist, DistAttr to fetch a batch of values and store it."""

    def __init__(self, generator, cache_num=20, symmetric=True):
        self.gen = generator
        self.cache = deepcopy(generator)
        self.num = cache_num
        self.symmetric = symmetric

    def __call__(self, *args):
        return self.fetch(*args)

    def fetch(self, *args):
        try:
            val = self._pop(*args)
        except (IndexError, AttributeError, TypeError):
            generator = self._get(self.gen, *args)

            if hasattr(generator, "params"):
                self._set(generator.generate(self.num), *args)
            else:
                self._set([generator] * self.num, *args)
            val = self._pop(*args)
        return val

    def _set(self, value, *args):
        if self.symmetric and len(args) == 2:
            if args[0] not in self.cache or args[1] not in self.cache.get(args[0]):
                self.cache[args[1]][args[0]] = value
            else:
                self.cache[args[0]][args[1]] = value
        elif len(args) == 0:
            self.cache = value
        else:
            self.cache[args[0]] = value

    def _get(self, val, *args):
        if self.symmetric and len(args) == 2:
            if args[0] not in val or args[1] not in val.get(args[0]):
                return val.get(args[1]).get(args[0])
            else:
                return val.get(args[0]).get(args[1])
        for attr in args:
            val = val.get(attr)
        return val

    def _pop(self, *args):
        last = None
        if len(args) == 0:
            last, self.cache = self.cache[-1], self.cache[:-1]
        elif len(args) == 1:
            last, self.cache[args[0]] = self.cache[args[0]][-1], self.cache[args[0]][:-1]
        elif self.symmetric and len(args) == 2:
            if args[0] not in self.cache or args[1] not in self.cache.get(args[0]):
                last, self.cache[args[1]][args[0]] = self.cache[args[1]][args[0]][-1], \
                                                     self.cache[args[1]][args[0]][:-1]
            else:
                last, self.cache[args[0]][args[1]] = self.cache[args[0]][args[1]][-1], \
                                                     self.cache[args[0]][args[1]][:-1]
        return last
