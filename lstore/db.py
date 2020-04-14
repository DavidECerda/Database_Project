from table import Table
from config import *
from diskmanager import DiskManager

### The database class ###
# Wraps the database and allows the user to open and close a database
# Also allows creation of new tables and iumporting old tables
class Database():

    # Initialized with a diskmanager class which handles reading and writing files from and to disk
    def __init__(self):
        self.tables = {}
        self.my_manager = DiskManager()
        self.my_manager.my_database = self
        pass
    
    ### Opens a existing database by reading files ###
    # :param path: string       #Path to database directory
    def open(self, path = "db_files"):
        self.my_manager.set_path(path)
        self.my_manager.open_db()
        pass
    
    ### Closes a database
    def close(self):
        self.my_manager.close_db()
        del self


    ### Creates a new table ###
    # :param name: string         #Table name
    # :param num_columns: int     #Number of Columns: all columns are integer
    # :param key: int             #Index of table key in columns

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.my_manager)
        self.tables[name] = table
        self.my_manager.make_table_folder(name)
        return table

   
    ### Deletes the specified table ###
    def drop_table(self, name):
        pass

    ### Imports a existing table from disk ###
    # :param name:              #Table name
    def get_table(self, name):
        print(self.tables[name].name)
        return self.my_manager.import_table(self.tables[name])