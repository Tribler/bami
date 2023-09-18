from bami.spar.sync_clock import ClockTable


def test_numpy_diff():
    import numpy as np

    # Define your arrays
    array1 = np.array([1, 5, 7, 10, 12])
    array2 = np.array([0, 4, 8, 9, 20])

    # Calculate the difference
    diff = np.abs(array1 - array2)

    # Sort the differences and get the indices
    sorted_indices = np.argsort(diff)[::-1]

    # Print the sorted differences and their indices
    print("Sorted differences:", diff[sorted_indices])
    print("Indices:", sorted_indices)


def test_clock_diff():
    c = ClockTable()
    c.increment(1)

    c2 = ClockTable()
    c2.increment(2)

    v = c2.compact_clock()
    v2 = ClockTable.from_compact_clock(v)

    vals = c.sorted_diff(v2)
    for val in vals:
        print(val)
