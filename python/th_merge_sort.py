#!/usr/bin/env python3
 
import threading
import time
import sys

""" Thread worker function """
def thread_work(i):
    print "sleeping 5 sec from thread %d" % i
    time.sleep(5)
    print "finished sleeping from thread %d" % i


def main():
    thread_handles = []

    thread = int(sys.argv[1])

    print "Threads selected: ", thread
    
    for i in range(thread):
        t = threading.Thread(target=thread_work, args=(i, ))
        thread_handles.append(t)
        t.start()

if __name__ == "__main__":
    main()
