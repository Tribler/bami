from typing import List, Tuple

import mmh3
import random

import numpy as np


class IBF:
    """Invertible bloom filter"""

    def __init__(self,
                 size: int = 32,
                 seed: int = 0,
                 sums_type: np.dtype = np.uint64
                 ):
        self.counters = np.array([0] * size, dtype=np.uint)
        self.sums = np.array([0] * size, dtype=sums_type)
        self.seed = seed
        self.size = size

    @staticmethod
    def from_arrays(counters: np.array, sums: np.array):
        c = IBF()
        c.counters = counters
        c.sums = sums
        return c

    def index(self, item_val: int) -> int:
        """Get cell id associated with the item"""
        return (item_val ^ self.seed) % self.size

    def add(self, value: int):
        i = self.index(value)
        self.counters[i] += 1
        self.sums[i] = np.uint(int(self.sums[i]) ^ value)
        return i

    def diff(self, other_ibf: 'IBF') -> 'IBF':
        c = self.counters - other_ibf.counters
        s = self.sums ^ other_ibf.sums
        return IBF.from_arrays(c, s)

    def peel(self, element_id: int, element_val: int):
        self.counters[element_id] -= 1
        self.sums[element_id] = self.sums[element_id] ^ element_val


def reconcile_ibfs(ibf1: IBF, ibf2: IBF) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]], IBF]:
    sim_diff = ibf1.diff(ibf2)
    peelable = True
    values1 = []
    values2 = []
    print(sim_diff.counters)
    for i in range(sim_diff.size):
        if sim_diff.counters[i] == 1 or sim_diff.counters[i] == -1:
            value = sim_diff.sums[i]
            sim_diff.peel(i, value)
            if sim_diff.counters[i] == 1:
                values1.append((i, value))
                sim_diff.counters[i] = sim_diff.counters[i] - 1
            else:
                values2.append((i, value))
                sim_diff.counters[i] = sim_diff.counters[i] + 1
    return values1, values2, sim_diff


class IBLT:
    """
    Simple implementation of an invertible bloom lookup table.
    The IBLT returned will have the format for a list of lists.
    Each list in an element, each element is of the form [idSum, hashSum, count]
    """
    _M = 20
    _K = 3
    SEED_RANGE = 1000000

    def __init__(self, m=_M, k=_K, seed_list=None, single_hash=None):
        """
        Constructor

        Args:
            m(int): Size of bloom filter array.
            k(int): Number of unique hashing algorithms to use.
        """
        random.seed()
        if seed_list is None:
            self.seed_list = []
            for i in range(k):
                self.seed_list.append(random.randint(0, self.SEED_RANGE))
        else:
            self.seed_list = seed_list
        self.m = m
        if single_hash is None:
            self.element_hash = random.randint(0, self.SEED_RANGE)
        else:
            self.element_hash = single_hash

    def generate_table(self, item_ids):
        """
        Given a list of item IDs, generate a corresponding IBLT
        Args:
            item_ids(list): A list of IDs for items to be included in IBLT.

        Returns:
            list: An invertible bloom lookup table in format list of lists.
        """
        bloom = [(0, 0, 0)] * self.m
        for item in item_ids:
            hash_values = []
            for seed in self.seed_list:
                hash_values.append(mmh3.hash128(str(item).encode(), seed))
            for hash_value in hash_values:
                index = hash_value % self.m
                id_sum = bloom[index][0] ^ item
                if bloom[index][1] == 0:
                    hash_sum = mmh3.hash128(str(item).encode(), self.element_hash)
                else:
                    hash_sum = bloom[index][1] ^ mmh3.hash128(str(item).encode(), self.element_hash)
                count = bloom[index][2] + 1
                bloom[index] = (id_sum, hash_sum, count)
        return bloom

    def compare_tables(self, table1, table2):
        """
        Compares 2 IBLTs and attempts to return the symmetric difference.
        Args:
            table1: Invertible bloom filter 1
            table2: Invertible bloom filter 2

        Returns:
            list list str:
                The symmetric difference of the IBLTs, list 1 is the extra elements from filter 1,
                    list 2 is the extra elements from filter 2, and a string to confirm if the
                    decoding was successful.
        """
        if len(table1) != len(table2):
            return False
        m = len(table1)
        table1_differences = []
        table2_differences = []
        table3 = [[0, 0, 0]] * m
        # Generate symmetric difference table
        for index in range(m):
            id_sum = table1[index][0] ^ table2[index][0]
            hash_sum = table1[index][1] ^ table2[index][1]
            count = table1[index][2] - table2[index][2]
            table3[index] = [id_sum, hash_sum, count]
        decodable = True
        while decodable is True:
            decodable = False
            for index in range(m):
                quick_check_pass = False
                element = table3[index]
                if element[2] == 1 or element[2] == -1:
                    element_hash = mmh3.hash128(str(element[0]).encode(), self.element_hash)
                    if element_hash == element[1]:
                        table3 = self.peel_element(element[0], table3, element[2])
                        decodable = True
                        if element[2] == 1:
                            table1_differences.append(element[0])
                        else:
                            table2_differences.append(element[0])
        success = "Success"
        for index in range(m):
            if table3[index][1] != 0:
                success = "Failed"
        # print("IBLT: %s" %success)
        return table1_differences, table2_differences, success

    def peel_element(self, element_id, table, alteration):
        """
        Peels a single element from a given IBLT.

        Args:
            element_id(int): The element to be peeled.
            table(list): The invertible bloom lookup table.
            alteration(int): The indicator as to which list this element was stored in (1 OR -1)

        Returns:
            list:
                An updated invertible bloom lookup table with the given element removed.
        """
        hash_values = []
        element_hash = mmh3.hash128(str(element_id).encode(), self.element_hash)
        for seed in self.seed_list:
            hash_values.append(mmh3.hash128(str(element_id).encode(), seed))
        for hash_value in hash_values:
            index = hash_value % self.m
            id_sum = table[index][0] ^ element_id
            if table[index][1] == 0:
                hash_sum = element_hash
            else:
                hash_sum = table[index][1] ^ element_hash
            count = table[index][2] - alteration
            table[index] = (id_sum, hash_sum, count)
        return table
