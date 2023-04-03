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