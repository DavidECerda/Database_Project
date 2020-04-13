from tree import *
from table import *

### Index class ###
# :brief    :           #A data strucutre holding indices for various columns of a table. 
#                       #Key column indexed by default, Other columns indexed through this object. 
#                       #Indices are B+ Trees from tree class.

class Index:

    ### Initiliazer function ###
    # :param table:     #Table the indices are being made for
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table # type : Table


    def is_indexed(self, column):
        return not (self.indices[column] == None)


    ### Locate by value ###
    # :param column_idx:    #Which column to use in search
    # :param value:         #Value to search the index with
    # :return list:         #The rids of all records with the given value on column "column" if value exists, otherwise None
    def locate(self, column, value):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")

        return tree.get_rid(value)

    ### Locates entry by rid ###
    # :param rid:           #Rid to search for
    # :param column:        #Which column to use in search
    # :return key:          #Key value of the rid if found, otherwise None
    def locate_by_rid(self,rid, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")

        return tree.find_by_rid(rid)
    
    ### Locate by range ###
    # :param begin:         #Start of range
    # :param end:           #End of range
    # :param column:        #Which column to use in search
    # :return list:         #the RIDs of all records with values in column "column" between "begin" and "end"
    def locate_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.bulk_search(begin,end)

    ### Locate by range ###
    # :param begin:         #Start of range
    # :param end:           #End of range
    # :param column:        #Which column to use in search
    # :return int:          #Sum of all records with values in column "column" between "begin" and "end"
    def sum_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.sum_range(begin, end, column)

    ### Insert in index ###
    # :param value:         #Value of key to be inserted
    # :param rid:           #RID corresponding to key value
    # :param column:        #Which column to use
    def insert(self, val, rid, column):
        tree = self.indices[column]
        if tree is None:
            raise Exception("This column is not indexed")
        tree.insert(val, rid)

    ### Remove from index ###
    # :param column:        #Which column to use
    # :param key:           #Value of key to be removed
    # :param rid:           #RID corresponding to key value
    def remove(self, column, key, rid):
        tree = self.indices[column]

        if tree is None:
            raise Exception("This column is not indexed")

        tree.remove(key, rid)
    
    ### Remove by RID ###
    # :param column:        #Which column to use
    # :param rid:           #RID to remove
    def remove_by_rid(self, column, rid):
        key = self.locate_by_rid(rid, column)

        if key != None:
            self.remove(column, key, rid)
        
    ### Update existing entry ###
    # :param column:        #Which column to use
    # :param new_key:       #Key value of updated entry
    # :param rid:           #RID corresponding to new key value
    def update_index(self, column, new_key, rid):

        self.remove_by_rid(column, rid)
        self.insert(column, new_key, rid)

    
    ### Creates index on specfic column ###
    # :param column_number:     #Which column to index
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

    ### Deletes index on specfic column ###
    # :param column_number:     #Which column to remove index
    def drop_index(self, column_number):
        self.indices[column_number] = None
 
