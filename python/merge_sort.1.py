#!/usr/bin/python
import sys
import subprocess
import csv
import os
import pandas as pd

from timeit import default_timer as timer

# Python program for implementation of MergeSort
 
# Merges two subarrays of arr[].
# First subarray is arr[l..m]
# Second subarray is arr[m+1..r]
def merge(arr, l, m, r):
    n1 = m - l + 1
    n2 = r- m
 
    # create temp arrays
    L = [0] * (n1)
    R = [0] * (n2)
 
    # Copy data to temp arrays L[] and R[]
    for i in range(0 , n1):
        L[i] = arr[l + i]
 
    for j in range(0 , n2):
        R[j] = arr[m + 1 + j]
 
    # Merge the temp arrays back into arr[l..r]
    i = 0     # Initial index of first subarray
    j = 0     # Initial index of second subarray
    k = l     # Initial index of merged subarray
 
    while i < n1 and j < n2 :
        if L[i] <= R[j]:
            arr[k] = L[i]
            i += 1
        else:
            arr[k] = R[j]
            j += 1
        k += 1
 
    # Copy the remaining elements of L[], if there
    # are any
    while i < n1:
        arr[k] = L[i]
        i += 1
        k += 1
 
    # Copy the remaining elements of R[], if there
    # are any
    while j < n2:
        arr[k] = R[j]
        j += 1
        k += 1
 
# l is for left index and r is right index of the
# sub-array of arr to be sorted
def merge_sort(arr,l,r):
    if l < r:
 
        # Same as (l+r)/2, but avoids overflow for
        # large l and h
        m = (l+(r-1))/2
 
        # Sort first and second halves
        merge_sort(arr, l, m)
        merge_sort(arr, m+1, r)
        merge(arr, l, m, r)
 



def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory


def write_file(input_file, write_row):
    with open(os.path.join('../dataset/', input_file), 'a') as file:
        writer = csv.writer(file)
        writer.writerow([write_row])


def main():
    print("Merge Sort - CSV File\n")

    # Whole row
    results = []
    # largest = 0


    # in_file = input("Enter CSV file name to sort: ") + ".csv"
    with open("/dataset/factbook2.csv") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        next(reader)
        for row in reader:  # each row is a list
            results.append(row)
            # country = row['country']
    
    # print(results)

    n = len(results)

# GET SERIAL TIME
    start = timer()

    print("\nPerforming merge sort on file: " +in_file)
    merge_sort(results, 0, n-1)

    end = timer()
    elapsed = end - start

    print("Total time elapsed = " + str(elapsed) + "\n")
    # print(results)

    out_file = input("Enter CSV file name: ") + ".csv"
    print("Saved as: ", out_file)
    print("\tCSV file found at:" + path_file(out_file))

    # write_file(out_file, results)

    my_df = pd.DataFrame(results)
    # my_df.columns = ["Country", "Death rate(deaths/1000 population)", "GDP"]
    my_df.to_csv('dataset/' + out_file, index=False)

    print(my_df)
    


if __name__ == "__main__":
    main()
