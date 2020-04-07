class Node:
    
    def __init__(self, order):
        self.order = order
        self.left = None
        self.right = None

        self.index = None
        self.rid = None
        self.is_leaf = False
    
    def split(self):
        pass
    def add(self):
        pass
    def if_full(self):
        pass
    def get(self):
        pass


class BplusTree:

    def __init__(self, order):
        self.root = None

    def find(self, node):
        pass
    def merge(self, parent, child, index):
        pass
    def insert(self,node):
        pass
    def get_value(self, key):
        pass