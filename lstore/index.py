from tree import *
from table import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table # type : Table

    def is_indexed(self, column):
        return not (self.indices[column] == None)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column_idx, value):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")

        return tree.get_rid(value)

    def locate_by_rid(self,rid, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")

        return tree.find_by_rid(rid)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """


    def locate_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.bulk_search(begin,end)

    def sum_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.sum_range(begin, end, column)

    def insert(self, val, rid, column_idx):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")
        tree.insert(val, rid)

    def remove(self, column_idx, key, rid):
        tree = self.indices[column_idx]

        if tree is None:
            raise Exception("This column is not indexed")

        tree.remove(key, rid)

    def remove_by_rid(self, column, rid):
        key = self.locate_by_rid(rid, column)

        if key != None:
            self.remove(column, key, rid)
        
    def update_index(self, column, new_key, rid):

        self.remove_by_rid(column, rid)
        self.insert(column, new_key, rid)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number >= self.table.num_columns:
            print("Out of range")
            return None

        self.indices[column_number] = BPlusTree(16)

        table_keys = list(self.table.key_index.keys())
        table_rids = list(self.table.key_index.values())

        table_col = [0 for _ in range(self.table.num_columns)]
        table_col[column_number] = 1

        for i in range(len(self.table.key_index)):
            fetched = self.table.select(table_keys[i], self.table.key_col, table_col)[0]
            value = fetched.columns[column_number]
            self.indices[column_number].insert(value,table_rids[i])

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
 
