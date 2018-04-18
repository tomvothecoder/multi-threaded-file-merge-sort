#!/usr/bin/python3
import sys
import subprocess
import csv
import os
import pandas as pd

def mergeSort(alist):
    # print("Splitting ", alist)
    if len(alist) > 1:
        mid = len(alist) // 2
        lefthalf = alist[:mid]
        righthalf = alist[mid:]

        mergeSort(lefthalf)
        mergeSort(righthalf)

        i = 0
        j = 0
        k = 0
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i][1] < righthalf[j][1]:
                alist[k] = lefthalf[i]
                i = i + 1
            else:
                alist[k] = righthalf[j]
                j = j + 1
            k = k + 1

        while i < len(lefthalf):
            alist[k] = lefthalf[i]
            i = i + 1
            k = k + 1

        while j < len(righthalf):
            alist[k] = righthalf[j]
            j = j + 1
            k = k + 1
    # print("Merging ", alist)


def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory


def write_file(input_file, write_row):
    with open(os.path.join('../dataset/', input_file), 'a') as file:
        writer = csv.writer(file)
        writer.writerow([write_row])


def main():
    # Whole row
    results = []
    # largest = 0

    with open("../dataset/factbook2.csv") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        next(reader)
        for row in reader:  # each row is a list
            results.append(row)
            # country = row['country']

    n = len(results)

    # for i in range(n):
    #     print("\nCurrent position: ", results[i][1])
    #     if (int(results[i][1]) > largest):
    #         largest = int(results[i][1])
    #     print("Current largest", largest)

    # print(results)

    # mergeSort(results, 0, n - 1)
    # mergeSort(results)
    # print(results)

    print("Merge Sort - CSV File\n")

    file_name = input("Enter CSV file name: ") + ".csv"
    print("Saved as: ", file_name)
    print("\tCSV file found at:" + path_file(file_name))

    # write_file(file_name, results)

    my_df = pd.DataFrame(results)
    my_df.columns =["Country", "Death rate(deaths/1000 population)" ,"GDP"]
    my_df.to_csv('../dataset/' + file_name, index=False)

    print(my_df)



if __name__ == "__main__":
    main()
