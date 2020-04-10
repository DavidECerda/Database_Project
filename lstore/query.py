from table import Table, MetaRecord
from index import Index
import time

def with_merge_lock(f):
    def wrapper(*args):
        query = args[0]
        table = query.table
        with table.merge_lock:
            return f(*args)

    wrapper.__name__ = f.__name__

    return wrapper
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    # @with_merge_lock
    def delete(self, key):
        return self.table.delete_record(key)

    """
    # Insert a record with specified columns
    """

    # INDIRECTION_COLUMN = 0
    # RID_COLUMN = 1
    # TIMESTAMP_COLUMN = 2
    # SCHEMA_ENCODING_COLUMN = 3

    # @with_merge_lock
    def insert(self, *columns):
        if len(columns) > self.table.num_columns:
            raise Exception('More arguments than columns')
        
        ok = self.table.create_row(columns)

        return ok

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    # @with_merge_lock
    def select(self, key, column, query_columns):
        return self.table.select(key, column, query_columns)

    """
    # Update a record with specified key and columns
    """

    # @with_merge_lock
    def update(self, key, *columns):
        return self.table.update_row(key, columns)


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    # @with_merge_lock
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_records(start_range,end_range,aggregate_column_index)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key_col, [1] * self.table.num_columns)
        # if r is not False:
        if len(r) > 0:
            r = r[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False