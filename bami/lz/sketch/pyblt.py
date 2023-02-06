"""Python wrapper for Gavin Andresen's c++-based IBLT implementation."""

__author__ = "Brian Levine"

from ctypes import *
from collections import defaultdict
import threading

import sys

assert sys.version_info.major > 2, 'This code requires python 3 or above.'

LIB = cdll.LoadLibrary('libpyblt.so')


class PYBLT():
    """
    Uses  All the functions take the pointer to the allocated c++ data Structure
    as the first argument.  (c_ulong is the pointer.)
    """
    POINTER = 0
    free_list = dict()  # used to avoid double-freeing a pointer in multithreaded applications
    fl_ind = 0
    FL_MAX = 10

    def __init__(self, entries, value_size, hedge=None, num_hashes=None, allocate=True):
        """
        entries: number of items to be recovered from the IBLT (not necessarily the number inserted)
        value_size: all stored values must be the same size
        hedge: If None, then autoset by c++; a multiplier on the number of cells.
        num_hashes: If None, then autoset by c++; number of hash functions used by IBLT.
        allocate: used internally, if False, don't call c++ function to allocate memory
        """

        # You have to set neither or both of hedge and num_hashes
        assert (hedge is None and num_hashes is None) or (hedge is not None and num_hashes is not None)

        self.value_size = value_size
        self.lock = threading.Lock()

        if (hedge is not None and num_hashes is not None):
            call = LIB.pyblt_manual
            call.argtypes = [c_int, c_int, c_float, c_int]
            call.restype = c_ulong
            if allocate:
                self.POINTER = call(entries, value_size, hedge, num_hashes)
        elif hedge is None and num_hashes is None:
            call = LIB.pyblt_new
            call.argtypes = [c_int, c_int]
            call.restype = c_ulong
            if allocate:
                self.POINTER = call(entries, value_size)

    def __del__(self):
        """ Deallocate memory"""
        call = LIB.pyblt_delete
        call.argtypes = [c_ulong]
        # hack: keep a list of pointer locations we've already free
        self.lock.acquire()
        if self.POINTER not in PYBLT.free_list:
            call(self.POINTER)
            PYBLT.free_list[self.fl_ind] = self.POINTER
            self.fl_ind = (self.fl_ind + 1) % self.FL_MAX
        self.lock.release()

    @staticmethod
    def set_parameter_filename(filename):
        """ Set the csv file to use that optimizes parameters """
        call = LIB.pyblt_set_parameter_file
        call.argtypes = [c_char_p]
        call(bytes(filename, "ascii"))

    def dump_table(self):
        """ Dump the internal representatino of the IBLT"""
        call = LIB.pyblt_dump_table
        call.argtypes = [c_ulong]
        call.restype = c_char_p
        result = call(self.POINTER)
        result = [x.decode() for x in result.split(b'\n')]
        return [x for x in result if x != '']

    def insert(self, key_int, value):
        """ Insert a new (key,value) pair.
        key_int: key must be an integer
        value: anything you want, but it will be converted to a string before storage
        """
        value = bytes(str("%" + str(self.value_size) + "s") % value, "ascii")

        assert type(value) == type(b''), "value is not a bytearray:  %s != %s" % (type(value), type(b''))
        assert len(value) == self.value_size, "value size is %d != %d" % (len(value), self.value_size)

        call = LIB.pyblt_insert
        call.argtypes = [c_ulong, c_ulong, c_char_p]
        call(self.POINTER, key_int, bytes(value.hex(), 'ascii'))

    def erase(self, key_int, value):
        """ Erase a new (key,value) pair.
        key_int: key must be an integer
        value: anything you want, but it will be converted to a string before storage
        """
        value = bytes(str("%" + str(self.value_size) + "s") % value, 'ascii')
        assert type(value) == type(b''), "value is not a bytearray:  %s != %s" % (type(value), type(b''))
        assert len(value) == self.value_size, "value size is %d != %d" % (len(value), self.value_size)

        call = LIB.pyblt_erase
        call.argtypes = [c_ulong, c_ulong, c_char_p]
        call(self.POINTER, key_int, bytes(value.hex(), 'ascii'))

    class RESULT(Structure):
        """ Return results from the IBLT that contain keys and values """
        # This must be in the same order as the C++ struct
        _fields_ = [
            ('decoded', c_bool),
            ("pos_len", c_uint),
            ("neg_len", c_uint),
            ("pos_keys", POINTER(c_ulonglong)),
            ("neg_keys", POINTER(c_ulonglong)),
            ("pos_str", c_char_p),
            ("neg_str", c_char_p)]

    class KEYS(Structure):
        """ Return results from the IBLT that contain keys only, no values """
        # This must be in the same order as the C++ struct
        _fields_ = [
            ("pos_len", c_uint),
            ("neg_len", c_uint),
            ("pos_keys", POINTER(c_ulonglong)),
            ("neg_keys", POINTER(c_ulonglong))]

    def list_entries(self):
        """ Decode the IBLT and list all entries (keys and values).
        The IBLT is left in tact.
        This is fragile in that a partially decodeable IBLT just returns None
        """
        call = LIB.pyblt_list_entries
        call.argtypes = [c_ulong]
        call.restype = self.RESULT
        result = call(self.POINTER)

        if result.pos_len > 0:
            pos_str = result.pos_str.decode().split()
        entries = defaultdict(dict)
        for x in range(result.pos_len):
            value = pos_str[x]
            entries[result.pos_keys[x]] = (bytes.fromhex(value).decode(), 1)

        if result.neg_len > 0:
            neg_str = result.neg_str.decode().split()
        for x in range(result.neg_len):
            value = neg_str[x]
            entries[result.neg_keys[x]] = (bytes.fromhex(value).decode(), -1)
        return result.decoded, entries

    def peel(self):
        """ Peel the IBLT in place. Meaning, any found entries are removed from the tableself.
        This function returns only keys, not values.
        """
        call = LIB.pyblt_peel_entries
        call.argtypes = [c_ulong]
        call.restype = self.KEYS
        result = call(self.POINTER)
        # print("pos len",result.pos_len)
        # print("neg len",result.neg_len)
        entries = list()
        for x in range(result.pos_len):
            entries += [result.pos_keys[x]]
        for x in range(result.neg_len):
            entries += [result.neg_keys[x]]
        return entries

    def subtract(self, other):
        """ call an "other" IBLT from this one, as per Eppstein. Not destructive. """
        assert type(other) == type(PYBLT(1, 1, 1, 2))
        call = LIB.pyblt_subtract
        call.argtypes = [c_ulong, c_ulong]
        call.restype = c_ulong
        # the call to subtract allocates new memory, we need to receive it.
        res = PYBLT(entries=None, value_size=self.value_size,
                    allocate=False)  # Nones are ignored when last argument is false
        res.POINTER = call(self.POINTER, other.POINTER)
        return res

    def get_serialized_size(self):
        """ Minimal size of the IBLT if serialized, in count of cells (rows). Doesn't include parameters. """
        call = LIB.pyblt_capacity
        call.argtypes = [c_ulong]
        call.restype = c_int
        res = call(self.POINTER)
        return res

# if __name__=='__main__':
# ss=PYBLT(10,20) 测试函数
