from table import Table
from config import *
from diskmanager import DiskManager

class Database():

    def __init__(self):
        self.tables = {}
        self.my_manager = DiskManager()
        self.my_manager.my_database = self
        pass

    def open(self, path = "db_files"):
        self.my_manager.set_path(path)
        self.my_manager.open_db()
        pass

    def close(self):
        self.my_manager.close_db()
        del self

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.my_manager)
        self.tables[name] = table
        self.my_manager.make_table_folder(name)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
    
    """
     # Returns table with the passed name
    """

    def get_table(self, name):
        print(self.tables[name].name)
        return self.my_manager.import_table(self.tables[name])