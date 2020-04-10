import threading
import logging


def with_protection(f):
    def wrapper(*args):
        lock_inst = args[0]
        with lock_inst.protection:
            return f(*args)

    return wrapper


class XLock:
    def __init__(self, resource):
        self.protection = threading.RLock()
        self.owner = None
        self.resource = resource

    @with_protection
    def acquire(self, block):
        '''
        User acquiring lock

        block: compatibility, ignored
        '''

        curr_thread = threading.currentThread()

        if curr_thread == self.owner:
            return True

        shared_count = self.resource.get_shared_count()

        if shared_count > 1:
            return False

        if shared_count == 1 and not self.resource.is_s_locked_by_curr():
            return False

        if self.owner is not None:
            return False

        self.owner = curr_thread
        return True

    @with_protection
    def release(self):

        curr_thread = threading.currentThread()

        if self.owner != curr_thread:
            raise Exception("Trying to release non-existant x lock")

        self.owner = None

        logging.debug("{}: {} {}".format("Released X Lock", id, curr_thread))

    def is_locked(self):
        return self.owner != None

    def get_owner(self):
        return self.owner


class SLock:
    def __init__(self, resource):
        self.protection = threading.RLock()
        self.owners = []
        self.resource = resource

    @with_protection
    def acquire(self, block):
        '''
        User acquiring lock

        block: compatibility, ignored
        '''

        curr_thread = threading.currentThread()

        if curr_thread in self.owners:
            return True

        x_owner = self.resource.get_x_owner()
        if x_owner is not None and x_owner != curr_thread:
            return False

        self.owners.append(curr_thread)
        return True

    @with_protection
    def release(self):

        curr_thread = threading.currentThread()

        try:
            idx = self.owners.index(curr_thread)
            del self.owners[idx]
        except ValueError:
            raise Exception("Trying to release non-existant s lock")

        logging.debug("{}: {} {}".format("Released S Lock", id, curr_thread))

    @with_protection
    def get_count(self):
        return len(self.owners)

    @with_protection
    def is_owned_by_curr(self):
        curr_thread = threading.currentThread()
        return curr_thread in self.owners

class Resource:
    def __init__(self, id):
        self.id = id
        self.x_lock = XLock(self)
        self.s_lock = SLock(self)

    def is_x_locked(self):
        return self.x_lock.is_locked()

    def get_shared_count(self):
        return self.s_lock.get_count()

    def get_x_owner(self):
        return self.x_lock.get_owner()

    def is_s_locked_by_curr(self):
        return self.s_lock.is_owned_by_curr()

class LockManager:
    def __init__(self):
        self.resources = {}
        self.protection = threading.RLock()

    @with_protection
    def create(self, id):
        self.resources[id] = Resource(id)

    @with_protection
    def get(self, id):
        if id not in self.resources:
            self.create(id)
        
        return self.resources[id]