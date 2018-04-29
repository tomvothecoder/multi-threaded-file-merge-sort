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
    # size = int(sys.argv[-1]) if sys.argv[-1].isdigit() else 1000
    # data_unsorted = [random.randint(0, size) for _ in range(size)]
    # for sort in merge_sort, merge_sort_parallel:
    #     start = time.time()
    #     data_sorted = sort(data_unsorted)
    #     end = time.time() - start
    # print (sort.__name__, end, sorted(data_unsorted) == data_sorted)

    print("Merge Sort - CSV File\n")

    # comm_sz = int(sys.argv[1])
    numProc = multiprocessing.cpu_count()
    print("Number of processors available on system: " + str(numProc))
   
    comm_sz = int(input("Enter number of workers per processor: "))

    # print("Number of workers/processor selected: ", comm_sz)

    # List used to store CSV (unsorted)
    csv_list = []
 # Read in the unsorted CSV file
    in_file = input("\nEnter unsorted CSV file name to sort: ") + ".csv"
    
    pd.set_option('display.width', 120)

    df = pd.read_csv(os.path.join('../input_dataset/', in_file), sep=",")

    plt.xlabel('Life Expectancy at Birth (years)')
    plt.ylabel('Death rate(deaths/1000 population)')

    df.columns = ["Country", "Unemployment rate(%)", "Death rate(deaths/1000 population)"]
    df = df[df['Unemployment rate(%)'].notnull()]
    df = df[df['Death rate(deaths/1000 population)'].notnull()]
   
    print(df)
    csv_list = df.values[1:].tolist()

    # n = len(csv_list)

    # SEQUENTIAL IMPLEMENTATIOMN
    print("\nPerforming sequential merge sort on file: " + in_file)
    seq_list = list(csv_list)
    start = timer()
    seq_list = merge_sort(seq_list)
    end = timer()
    elapsed = end - start
    print("SEQUENTIAL total time elapsed: " + str(elapsed) + "\n")

    #PARALLEL IMPLEMENTATION
    print("\nPerforming parallel merge sort on file: " + in_file)
    par_list = list(csv_list)
    start = timer()
    par_list = merge_sort_parallel(par_list, comm_sz)
    end = timer()
    elapsed = end - start
    print("PARALLEL total time elapsed: " + str(elapsed) + "\n")

    # Output the list
    out_file = input("Enter Sorted CSV file name (OUTPUT): ") + ".csv"

    print("Saved as: ", out_file)
    print("\tCSV file found at:" + path_file(out_file))

# Export to output csv using pandas
    write_file_pandas(par_list, out_file)
    plotter(out_file)


def merge(*args):
    # Support explicit left/right args, as well as a two-item
    # tuple which works more cleanly with multiprocessing.
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

    # numProc = multiprocessing.cpu_count()
    # print("Number of processors available: " + str(numProc))
   
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

def plotter(input_file):
    # Axis for arrays
    x = []
    y = []

    graph_file = input("\nEnter graph file name: ") + ".png"
    print("\t Saved as: ", graph_file)
    print("\tGraph found at:" + path_file(graph_file))
        
    df = pd.read_csv(os.path.join('../output_dataset/', input_file), sep=",")
    # print(df)

    plots = df.values.tolist()
    
    for row in plots:
        x.append(int(row[1]))
        y.append(int(row[2]))

    fig, ax = plt.subplots(1, 1)

    tick_spacing = 5  # Spacing for x Axis
    locator = plticker.MultipleLocator(tick_spacing)
    # locator.MAXTICKS = 10000

    ax.scatter(x, y)
#    ax.plot(x, y, label="Census statistics, Death Rate vs. GDP")
    plt.xlabel('Unemployment rate(%)')
    plt.ylabel('Death rate(deaths/1000 population)')

    ttl=plt.title('Death Rates per Country in Relation to Pop. Unemployment %')
    ttl.set_position([.5, 1.05])
    plt.legend()
    ax.xaxis.set_major_locator(locator)  # Sets the spacing
    plt.xticks(fontsize=6, rotation=0)  # font size and rotation

    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    plt.plot(x,p(x),"r--")

    plt.show()
    plt.savefig(os.path.join('../output_dataset/graphs/' + graph_file), bbox_inches="tight")



def write_file_pandas(arr, out_file):
    my_df = pd.DataFrame(arr)
    my_df.columns = ["Country", "Unemployment rate(%)", "Death rate(deaths/1000 population)"]

    # my_df.columns = ["Country", "Death rate(deaths/1000 population)", "GDP"]
    my_df.to_csv('../output_dataset/' + out_file, index=False)

    print(my_df)


def read_file_list(arr, in_file):
    with open("../input_dataset/" + in_file) as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        next(reader)
        for row in reader:  # each row is a list
            arr.append(row)
            # country = row['country']


def write_file(input_file, write_row):
    with open(os.path.join('../output_dataset/', input_file), 'a') as file:
        writer = csv.writer(file)
        writer.writerow([write_row])

def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory

if __name__ == "__main__":
    main()
