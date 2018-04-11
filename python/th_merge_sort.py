##!/usr/bin/env python3
import threading
import time
import sys

lock = threading.RLock()

""" Thread worker function """
def thread_work(i):
    lock.acquire()
    print ("Thread lock acquired")
    print("Sleeping 1 sec from thread: ", i)
    time.sleep(1)
    lock.release()
    print ("Thread lock released")
    print("Finished sleeping from thread: ", i)


def main():
    thread_handles = []
    thread = int(sys.argv[1])
    print ("Threads selected: ", thread)
    
    for i in range(thread):
        t = threading.Thread(target=thread_work, args=(i, ))
        thread_handles.append(t)
        t.start()

if __name__ == "__main__":
    main()
