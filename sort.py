# !/usr/bin/python3
__author__ = "Tom Vo"
__version__ = "0.1.0"

import math
import multiprocessing
from timeit import default_timer as timer

from plot import input_to_df, path_file, output_csv, plot_output


def merge(*args):
    """
    Performs the merging operation
    Args:
        *args:

    Returns:

    """
    left, right = args[0] if len(args) == 1 else args
    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:

        if left[left_index][1] <= right[right_index][1]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    if left_index == left_length:
        merged.extend(right[right_index:])
    else:
        merged.extend(left[left_index:])
    return merged


def sort(data):
    """
    Performs the serial merge sort
    :param data:
    :return:
    """
    length = len(data)
    if length <= 1:
        return data
    middle = length // 2
    left = sort(data[:middle])
    right = sort(data[middle:])
    return merge(left, right)


def sort_parallel(data, processes):
    """
    Performs the parallel merge sort using multiprocessing.
    Creates a pool of worker processes depending on user input.
    Split the unsorted dataset into partitions evenly among workers
    Perform a local merge sort across each partition.

    Each partition is now sorted - we now just merge pairs of these
    together using the worker pool, until the partitions are reduced
    down to a single sorted result.

    If the number of partitions remaining is odd, pop off the
    last one and append it back after one iteration of this loop,
    since we're only interested in pairs of partitions to merge.
    Args:
        data:
        processes:

    Returns:

    """
    pool = multiprocessing.Pool(processes=processes)

    # Evenly divide the number of data from the list among the workers
    size = int(math.ceil(float(len(data)) / processes))
    data = [data[i * size:(i + 1) * size] for i in range(processes)]
    data = pool.map(sort, data)

    while len(data) > 1:
        extra = data.pop() if len(data) % 2 == 1 else None
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data) + ([extra] if extra else [])
    return data[0]


def main():
    print("Merge Sort - CSV File\n")
    num_proc = multiprocessing.cpu_count()
    print("Number of processors available on system: " + str(num_proc))

    comm_sz = int(input("Enter number of workers (processors): "))
    file_name = input("\nEnter unsorted CSV file name to sort: ") + ".csv"
    csv_list = input_to_df(file_name)

    # SEQUENTIAL IMPLEMENTATIOMN
    print("\nPerforming sequential merge sort")
    seq_list = list(csv_list)
    start = timer()
    seq_list = sort(seq_list)
    end = timer()
    elapsed = end - start
    print("SEQUENTIAL total time elapsed: " + str(elapsed) + "\n")

    # PARALLEL IMPLEMENTATION
    print("\nPerforming parallel merge sort on file")
    par_list = list(csv_list)
    start = timer()
    par_list = sort_parallel(par_list, comm_sz)
    end = timer()
    elapsed = end - start
    print("PARALLEL total time elapsed: " + str(elapsed) + "\n")

    # Output the list
    out_file = f'{file_name.replace(".csv", "")}_out.csv'
    print("Saved as: ", out_file)
    print("\tCSV file found at:" + path_file(out_file))

    # Export to output csv
    output_csv(par_list, out_file)
    plot_output(out_file)


if __name__ == "__main__":
    main()
