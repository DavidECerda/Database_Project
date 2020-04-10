import threading

arr = [1, 2, 3, 4, 5]


def a():
    arr[0] = 6


def b():
    arr[1] = 7


def c():
    arr[2] = 8


def d():
    arr[3] = 9


def e():
    arr[4] = 10

def incr(i, j):
    while arr[i] != j:
        arr[i] += 1

threads = []

def add_thread(i, j):
    threads.append(threading.Thread(target=incr, args=(i, j)),)

add_thread(0,10)
add_thread(1,20)
add_thread(2,20)
add_thread(3,30)
add_thread(4,40)

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print(arr)