#!/usr/bin/python3
import sys
import subprocess
import csv
import os
import pandas as pd

from timeit import default_timer as timer

def merge_sort(alist):
    print("Splitting ",alist)
    if len(alist)>1:
        mid = len(alist)//2
        lefthalf = alist[:mid]
        righthalf = alist[mid:]

        merge_sort(lefthalf)
        merge_sort(righthalf)


    # Merge the temp arrays back into arr[l..r]

        i=0 # Initial index of first subarray
        j=0 # Initial index of second subarray
        k=0 # Initial index of merged subarray
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i][1] < righthalf[j][1]:
                alist[k]=lefthalf[i]
                i=i+1
            else:
                alist[k]=righthalf[j]
                j=j+1
            k=k+1

        while i < len(lefthalf):
            alist[k]=lefthalf[i]
            i=i+1
            k=k+1

        while j < len(righthalf):
            alist[k]=righthalf[j]
            j=j+1
            k=k+1
    print("Merging ",alist)


def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory


def write_file(input_file, write_row):
    with open(os.path.join('../dataset/', input_file), 'a') as file:
        writer = csv.writer(file)
        writer.writerow([write_row])

def read_file_list(arr, in_file):

    with open("dataset/" + in_file) as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        next(reader)
        for row in reader:  # each row is a list
            arr.append(row)
            # country = row['country']


def main():
    print("Merge Sort - CSV File\n")

    # Whole row
    results = []
    # largest = 0


    in_file = input("Enter CSV file name to sort: ") + ".csv"

    read_file_list(results, in_file)
    
    # print(results)

    n = len(results)

# GET SERIAL TIME
    start = timer()

    print("\nPerforming merge sort on file: " +in_file)
    merge_sort(results)

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
    my_df.to_csv('/dataset/' + out_file, index=False)

    print(my_df)
    


if __name__ == "__main__":
    main()
