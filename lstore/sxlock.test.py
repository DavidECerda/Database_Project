from sxlock import *
import threading
from time import sleep
import util

# lock = Lock(0)

# class SharedLock(threading.Thread):
#     def __init__(self):
#         super(SharedLock, self).__init__()

#     def run(self):
#       suc = lock.acquire_s_lock(self)
#       if suc:
#         print('Acquired shared lock')
#       else:
#         print("Couldn't acquire shared lock")

# class LoopSharedLock(threading.Thread):
#     def __init__(self):
#         super(LoopSharedLock, self).__init__()

#     def run(self):
#       while(True):
#         suc = lock.acquire_s_lock(3)
#         if suc:
#           print('Acquired shared lock')
#         else:
#           print("Couldn't acquire shared lock")

# class Thread4(threading.Thread):
#   def __init__(self):
#     super(Thread4, self).__init__()

#   def run(self):
#     suc = lock.acquire_x_lock(4)
#     if suc:
#       print('Acquired x lock')
#     else:
#       print("Couldn't acquire x lock")

#     sleep(3)
    
#     lock.release_x_lock(4)

# class Thread5(threading.Thread):
#   def __init__(self):
#     super(Thread5, self).__init__()

#   def run(self):
#     for _ in range(10):
#       suc = lock.acquire_x_lock(5)
#       if suc:
#         print('Acquired x lock')
#         break
#       else:
#         print("Couldn't acquire x lock")
#       sleep(1)
  



# threads = [
#   Thread4(),
#   SharedLock(),
#   SharedLock(),
#   # LoopSharedLock(),
#   Thread5(),
# ]

# for thread in threads:
#   thread.start()

# for thread in threads:
#   thread.join()

# a = Resource()
# b = Resource()
# c = Resource()
# d = Resource()

# def thread_a():
#     acquire_resp = acquire_all([a.x_lock, b.s_lock, c.s_lock])
#     if acquire_resp is False:
#         print("A: Couldn't acquire all locks")
#         return

#     locks = acquire_resp
#     print("A: Acquired all locks")

#     sleep(5)
#     release_all(locks)
#     print("A: Released all locks")

# def thread_b():
#     acquire_resp = acquire_all([d.x_lock, c.s_lock, b.x_lock])
#     if acquire_resp is False:
#         print("B: Couldn't acquire all locks")
#         return

#     locks = acquire_resp
#     print("B: Acquired all locks")

#     sleep(5)
#     release_all(locks)
#     print("B: Released all locks")

# def thread_c():
#     sleep(8)

#     acquire_resp = acquire_all([d.x_lock])
#     if acquire_resp is False:
#         print("C: Couldn't acquire all locks")
#         return

#     locks = acquire_resp
#     print("C: Acquired all locks")

#     sleep(5)
#     release_all(locks)
#     print("C: Released all locks")

# threads = [threading.Thread(target=thread_a, args=()), threading.Thread(target=thread_b, args=()), threading.Thread(target=thread_c, args=())]

# for thread in threads:
#   thread.start()

# for thread in threads:
#   thread.join()

lm = LockManager()
a = lm.get("a")
b = lm.get("b")
c = lm.get("c")

results = util.acquire_all([a.s_lock, a.x_lock, b.x_lock, c.x_lock])
print(results)
