#!/usr/bin/python3
import math
import multiprocessing
import random
import sys
import csv
import os

import numpy as np
import pandas as pd
from timeit import default_timer as timer
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker

def main():

    size = int(input("Enter number of elements: "))
    numProc = multiprocessing.cpu_count()

    print("Number of processors available on system: " + str(numProc))
   
    comm_sz = int(input("Enter number of workers (max above): "))

    data_unsorted = [random.randint(0, size) for _ in range(size)]
    # data_unsorted = [[random.random() for i in range(size)] for j in range (3)]
    seq_list = list(data_unsorted)
    start = timer()
    merge_sort(seq_list)
    end = timer()
    elapsed = end - start
    print("SEQUENTIAL total time elapsed: " + str(elapsed) + "\n")


    par_list = list(data_unsorted)
    start = timer()
    merge_sort_parallel(par_list, comm_sz)
    end = timer()
    elapsed = end - start
    print("PARALLEL total time elapsed: " + str(elapsed) + "\n")

def merge(*args):
    # Support explicit left/right args 
    # Two item tuple-> works cleanly with multiprocessing.
    left, right = args[0] if len(args) == 1 else args
    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:

        if left[left_index] <= right[right_index]:
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



def merge(*args):
    # Support explicit left/right args 
    # Two item tuple-> works cleanly with multiprocessing.
    left, right = args[0] if len(args) == 1 else args
    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:

        if left[left_index]<= right[right_index]:
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


# SERIAL MERGE_SORT
def merge_sort(data):
    length = len(data)
    if length <= 1:
        return data
    middle = length // 2
    left = merge_sort(data[:middle])
    right = merge_sort(data[middle:])
    return merge(left, right)


# PARALLEL MERGE_SORT
def merge_sort_parallel(data, processes):

    # Creates a pool of worker processes depending on user input.
    # Split the unsorted dataset into partitions evenly among workers
    # Perform a local merge sort across each partition.
   
    pool = multiprocessing.Pool(processes=processes)

    # Evenly divide the number of data from the list among the workers
    size = int(math.ceil(float(len(data)) / processes))
    data = [data[i * size:(i + 1) * size] for i in range(processes)]
    data = pool.map(merge_sort, data)

    # Each partition is now sorted - we now just merge pairs of these
    # together using the worker pool, until the partitions are reduced
    # down to a single sorted result.
    while len(data) > 1:
        # If the number of partitions remaining is odd, we pop off the
        # last one and append it back after one iteration of this loop,
        # since we're only interested in pairs of partitions to merge.
        extra = data.pop() if len(data) % 2 == 1 else None
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data) + ([extra] if extra else [])
    return data[0]

if __name__ == "__main__":
    main()
