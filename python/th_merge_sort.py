#!/usr/bin/python3
from multiprocessing import Process, Pipe
import sys
import csv
import os
import pandas as pd
import time

from timeit import default_timer as timer


def main():

    print("Merge Sort - CSV File\n")

    comm_sz = int(sys.argv[1])

    print("Number of processors selected: ", comm_sz)

    # List used to store CSV (unsorted)
    csv_list = []

    in_file = input("Enter unsorted CSV file name to sort: ") + ".csv"
    read_file_list(csv_list, in_file)

    # print(results)
    n = len(csv_list)

    # GET SERIAL TIME
    print("\nPerforming sequential merge sort on file: " + in_file)
    seq_list = list(csv_list)

    start = time.time()
    
    seq_list = mergesort(csv_list)
    

    end = time.time()
    elapsed = end - start

    if not isSorted(seq_list):
        print('Sequential mergesort did not sort. oops.')

    print("SEQUENTIAL total time elapsed: " + str(elapsed) + "\n")
    # print(results)

    #PARALLEL IMPLEMENTATION
    print("\nPerforming parallel merge sort on file: " + in_file)

    par_list = list(csv_list)
    start = time.time()

    #Start process, send it to the entire list along with pipe to receive response
    pconn, cconn = Pipe()
    p = Process(target=mergeSortParallel, \
        args=(par_list, cconn, comm_sz))

    p.start()
    par_list = pconn.recv()

    # Blocks until sorted list is received
    p.join()

    end = time.time()
    elapsed = end - start
    print("PARALLEL total time elapsed: " + str(elapsed) + "\n")

    out_file = input("Enter Sorted CSV file name: ") + ".csv"
    print("Saved as: ", out_file)
    print("\tCSV file found at:" + path_file(out_file))

    my_df = pd.DataFrame(par_list)
    my_df.columns = ["Country", "Death rate(deaths/1000 population)", "GDP"]
    my_df.to_csv('../dataset/' + out_file, index=False)

    print(my_df)


def merge(left, right):
    """returns a merged and sorted version of the two already-sorted lists."""
    ret = []
    li = ri = 0
    while li < len(left) and ri < len(right):
        if left[li][1] <= right[ri][1]:
            ret.append(left[li])
            li += 1
        else:
            ret.append(right[ri])
            ri += 1
    if li == len(left):
        ret.extend(right[ri:])
    else:
        ret.extend(left[li:])
    return ret


def mergesort(lyst):
    """
    The seemingly magical mergesort. Returns a sorted copy of lyst.
    Note this does not change the argument lyst.
    """
    if len(lyst) <= 1:
        return lyst
    ind = len(lyst) // 2
    return merge(mergesort(lyst[:ind]), mergesort(lyst[ind:]))
   

def mergeSortParallel(lyst, conn, procNum):
    """mergSortParallel receives a list, a Pipe connection to the parent,
       and procNum. Mergesort the left and right sides in parallel, then 
       merge the results and send over the Pipe to the parent."""

    #Base case, this process is a leaf or the problem is
    #very small.
    if procNum <= 0 or len(lyst) <= 1:
        conn.send(mergesort(lyst))
        conn.close()
        return
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

    ind = len(lyst) // 2

    #Create processes to sort the left and right halves of lyst.

    #In creating a child process, we also create a pipe for that
    #child to communicate the sorted list back to us.
    pconnLeft, cconnLeft = Pipe()
    leftProc = Process(target=mergeSortParallel, \
                       args=(lyst[:ind], cconnLeft, procNum - 1))

    #Creat a process for sorting the right side.
    pconnRight, cconnRight = Pipe()
    rightProc = Process(target=mergeSortParallel, \
                       args=(lyst[ind:], cconnRight, procNum - 1))

    #Start the two subprocesses.
    leftProc.start()
    rightProc.start()

    #Recall that expression execution goes from first evaluating
    #arguments from inside to out.  So here, receive the left and
    #right sorted sublists (each receive blocks, waiting to finish),
    #then merge the two sorted sublists, then send the result
    #to our parent via the conn argument we received.
    conn.send(merge(pconnLeft.recv(), pconnRight.recv()))
    conn.close()

    #Join the left and right processes.
    leftProc.join()
    rightProc.join()


def isSorted(lyst):
    """
    Return whether the argument lyst is in non-decreasing order.
    """
    #Cute list comprehension way that doesn't short-circuit.
    #return len([x for x in
    #            [a - b for a,b in zip(lyst[1:], lyst[0:-1])]
    #            if x < 0]) == 0
    for i in range(1, len(lyst)):
        if lyst[i][1] < lyst[i - 1][1]:
            return False
    return True


def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory


def write_file(input_file, write_row):
    with open(os.path.join('../dataset/', input_file), 'a') as file:
        writer = csv.writer(file)
        writer.writerow([write_row])


def read_file_list(arr, in_file):

    with open("../dataset/" + in_file) as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        next(reader)
        for row in reader:  # each row is a list
            arr.append(row)
            # country = row['country']


if __name__ == "__main__":
    main()
