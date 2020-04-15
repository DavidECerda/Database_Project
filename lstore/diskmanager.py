from config import *
from util import *
from page_rw_utils import *
from table import MetaRecord, Table
from pagerange import PageRange
from page import Page
import os

PAGE_OFFSET = 1
PAGE_RANGE_OFFSET = 2
NUMBER_OF_DEXS = 3

### Diskmanger Class ###
# This class manages reading and writing data from and to disk
# Writes the data as binary files and allows data to exist in
# More durable disk memory 
class DiskManager:

    ### Initizier Function ###
    # :param dir: string        #Path to the directory of the database files. Default is "database_files"
    # :IV self.my_database: db  #Reference to the database using the diskmanager object
    #                           #When db class is initialized it will create a diskmanager object
    #                           #And set self.my_database equal to itself
    # :brief    :               #Has a refernce to all the pageranges to allow
    #                           #Loading and Unloading pages to correct location in disk
    def __init__(self,  dir = "database_files"):
        self.my_database = None # type : db
        self.page_ranges = []

        if(dir[-1] != '/'):
            dir += '/'
            
        self.database_folder = dir


    ### Sets the path to database files ###
    # :param path: string       #Path to database files
    def set_path(self, path):
        access_rights = 0o755
        if(path[-1] != '/'):
            path += '/'
        path_list = path.split('/')
        if path_list[0] == '~':
            path_list[0] = os.getenv('HOME')
        self.database_folder = '/'.join(path_list)

    ### Creates a table folder ###
    # :param table_name: string        #Name of table to create folder for
    # :brief                    #Creates a folder for table files and makes name directory safe
    def make_table_folder(self, table_name):
        path = self.database_folder + sanitize(table_name)
        access_rights = 0o755
        try:
            os.mkdir(path, access_rights)
        except OSError:
            # print ("Creation of the directory %s failed" % path)
            pass
        else:
            # print ("Successfully created the directory %s" % path)
            pass


    ### Sets up everything needed to write a page to disk ###
    # :param table_name: string       #Name of table that owns page
    # :param page_pid: tuple          #Tuple of three consisting of (cell_idx, page_idx, page_range_idx
    def write_to_disk(self, table_name, page_pid):
        _, page_idx, page_range_idx = page_pid
        try:
            current_table = self.my_database.tables[table_name] # type: Table
        except KeyError:
            return False
        
        page = current_table.get_page(page_pid) # type: Page
        pagerange = current_table.get_page_range(page_range_idx)
        
        if page_idx >= PAGE_RANGE_MAX_BASE_PAGES:
            type_of_page = 'tail'
        else:
            type_of_page = 'base'

        self.write_page(pagerange, page, page_idx, type_of_page, table_name)

    ### Loads an existing database ###
    # :returns bool:    #True if database was loaded successfully, false otherwise
    # :brief    :       #Makes database folder if it doesn't exist
    #                   #Trys to find database directory files that has the metadata of the database
    #                   #Returns false if file is not found, otherwise
    #                   #Proceeds to read database directory file in frames of size CELL_SIZE_BYTES
    def open_db(self):
        try:
            os.mkdir(self.database_folder)
        except OSError:
            # print ("Creation of the directory %s failed" % self.database_folder)
            pass
        else:
            print ("Successfully created the directory %s" % self.database_folder)
            pass

        try: 
            database_directory_file = open(self.database_folder + "Database_Directory", 'r+b')
        except FileNotFoundError:
            return False

        num_of_tables = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))

        for i in range(num_of_tables):
            table_name_len = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            table_name = database_directory_file.read(table_name_len).decode('utf-8')
            key_col = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            num_columns = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))

            new_table = Table(table_name,num_columns,key_col, self)

            num_page_ranges = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            new_table.page_ranges = [None]*num_page_ranges

            for i in range(num_page_ranges):
                new_table.page_ranges[i] = self.import_page_ranges(i, table_name)

            self.my_database.tables[table_name] = new_table
            print(database_directory_file.read(CELL_SIZE_BYTES).decode('utf-8'))
        
        database_directory_file.close()

        return True

    ### Writes a database directory files ###
    #       --- db_directory_file format ---
    #
    #       Every piece of info is in 8 bytes (except table name).
    #       Outline of file
    #       - Number of tables (8 bytes)
    #       For each table
    #       - Length of table name (8 bytes)
    #       - Table Name (utf-8 encoded)
    #       - Key Column (which column contains the primary key colunm)
    #       - Number of columns if the table
    #       - Number of pageranges in the table
    def write_db_directory(self):
        binary_file = open(self.database_folder + "Database_Directory", 'w+b')

        data = bytearray()

        data += int_to_bytes(len(self.my_database.tables))
        self.separator = int_to_bytes(int_from_bytes('NewTable'.encode('utf-8')))

        for name, table in self.my_database.tables.items():
            data += int_to_bytes(len(name.encode('utf-8'))) + name.encode('utf-8')
            data += int_to_bytes(table.key_col)
            data += int_to_bytes(table.num_columns)
            data += int_to_bytes(len(table.page_ranges))
            data += self.separator

        binary_file.write(data)
        binary_file.close()

    ### Closes a database and writes the data to disk ###
    # :brief    :       #Writes a db directory files
    #                   #Writes a table meta file for each table
    #                   #Writes the page ranges of each table
    #                   #Deletes the database from RAM
    def close_db(self):
        self.write_db_directory()


        for table_name, table in self.my_database.tables.items():
            self.write_table_meta(table_name)

            for i, pagerange in enumerate(table.page_ranges):
                self.write_page_range(pagerange, i, table_name)
        del self.my_database
        
    # Todo
    def purge_table(self, table_name):
        pass

    # Todo
    def purge(self):
        pass

    ### Writes a table meta file ###
    # :param table_name: string     #Name of table to write file for
    # :returns bool:                #True if table exists, false otherwise
    #
    #         --- Meta_file format ---
    # Every piece of info is 8 bytes.
    # Outline of file.
    # - Prev_rid
    # - Prev_tid
    # - Page_Directory Length
    # - Num of rows
    # - Deleted records
    #   - Special code indicating start of deleted records
    #   - Number of deleted records
    #   - Special key value of deleted record
    #   - Schema (what to ignore)
    #   - Columns
    #   - Special code indicating end of deleted records or no deleted records
    # - MetaRecords
    #   - BaseRecords
    #       - Rid
    #       - Key
    #       - Columns
    #
    #   - TailRecords
    #       - Rid
    #       - Key
    #       - Schema
    #       - Columns
    def write_table_meta(self, table_name):
        try:  
            table = self.my_database.tables[table_name]
        except KeyError:
            return False

        try:
            binary_file = open(self.database_folder + sanitize(table_name) + '/' + sanitize(table_name) + "_meta", 'w+b')
        except FileNotFoundError:
            self.make_table_folder(table_name)
            binary_file = open(self.database_folder + sanitize(table_name) + '/' + sanitize(table_name) + "_meta", 'w+b')

        data = bytearray()

        # Write some globals
        data += int_to_bytes(table.prev_rid) # rid
        data += int_to_bytes(table.prev_tid) # tid
        data += int_to_bytes(len(table.page_directory))
        data += int_to_bytes(table.num_rows)

        # Writing deleted records with special key d0000000
        if 0 in table.page_directory:
            # print("Writing deleted records")

            data += 'bdeleted'.encode('utf-8')
            
            deleted_records = table.page_directory.pop(0)

            data += int_to_bytes(len(deleted_records)) # num deleted records
            key = 'd0000000'.encode('utf-8')

            for d_record in deleted_records:

                data += key
                the_columns = bytearray()
                schema = 0

                for i, column in enumerate(d_record.columns):

                    if column == None:
                            continue
                    else:

                        if i >= START_USER_DATA_COLUMN:
                            schema += 2**(i - START_USER_DATA_COLUMN)

                        for dex in column:
                            the_columns += int_to_bytes(dex)

                # Tells import_table which columns to ignore
                schema = int_to_bytes(schema)
                data += schema + the_columns

            data += 'edeleted'.encode('utf-8')

        else:
            data += 'nodelete'.encode('utf-8')

        # key is the rid and value are the metarecords
        # columns point to the location in data
        num_of_rids = len(table.page_directory)
        rids = list(table.page_directory.keys()) # type : dict
        metarecords = list(table.page_directory.values())
        
        tail_flag = False
        counter = 0

        # Write all Meta records
        while counter < num_of_rids:

            current_rid = rids[counter]
            current_record = metarecords[counter]

            tail_flag = current_rid > table.prev_rid

            # Write rid and key
            data += int_to_bytes(current_rid)
            data += int_to_bytes(current_record.key)

            # Write columns of base record
            if not tail_flag:

                for column in current_record.columns:

                    for dex in column:
                        data += int_to_bytes(dex)

            # Write columns of tail record
            else:
                the_columns = bytearray()
                schema = 0

                for i, column in enumerate(current_record.columns):

                    if column == None:
                        continue
                    else:
                        if i >= START_USER_DATA_COLUMN:
                            schema += 2**(i - START_USER_DATA_COLUMN)
                        for dex in column:
                            the_columns += int_to_bytes(dex)

                # Tells import_table which columns to ignore
                schema = int_to_bytes(schema)


                data += schema + the_columns

            counter += 1
        
        binary_file.write(data)
        binary_file.close()

        return True

    ### Imports a table from disk ###
    # :param table:     #Table to write to disk
    # :returns:         #The loaded table if meta file exists, false otherwise
    # :brief    :       #Reads table infor bases on metafile format (refer to write_table_meta)
    def import_table(self, table):
        try:
            meta_file = open(self.database_folder + sanitize(table.name) + '/' + sanitize(table.name) + '_meta', 'r+b')
        except FileNotFoundError:
            return False

        table.prev_rid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
        table.prev_tid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
        page_directory_size = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
        table.num_rows = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))

        if meta_file.read(CELL_SIZE_BYTES).decode('utf-8') == 'bdeleted':

            num_deleted_records = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))

            deleted_records = []

            for i in range(num_deleted_records):

                key = meta_file.read(CELL_SIZE_BYTES).decode()
                #if key == 'd0000000':
                    #print("Special key", key)
                    
                columns = [None for _ in range(table.num_total_cols)]
                
                schema = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                schema_encoding = bin(schema)[2:].zfill(table.num_columns)[::-1]

                for i in range(table.num_total_cols):
                            
                    if i >= START_USER_DATA_COLUMN and schema_encoding[i - START_USER_DATA_COLUMN] == '0':
                        continue
                    else:
                        column = []
                        for j in range(NUMBER_OF_DEXS):
                            val = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                            column.append(val)

                        columns[i] = column

                metarecord = MetaRecord(0,key,columns)
                deleted_records.append(metarecord)
                
            if  meta_file.read(CELL_SIZE_BYTES).decode('utf-8') == 'edeleted':
                table.page_directory[0] = deleted_records
                print("End deleting records")
            else:
                print("Oops")
        
        tail_flag = False

        for i in range(page_directory_size):

            # Read rid and key
            rid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
            key = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))

            tail_flag = rid > table.prev_rid

            columns = [None for _ in range(table.num_total_cols)]
            
            # print('tail flag', tail_flag)
            # Read columns of base record

            if not tail_flag:
                for i in range(table.num_total_cols):          
                    column = []

                    for j in range(NUMBER_OF_DEXS):
                        val = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                        column.append(val)

                    columns[i] = column

            # Read columns of tail record
            else:

                # Tells which columnbs to skip
                schema = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                schema_encoding = bin(schema)[2:].zfill(table.num_columns)[::-1]

                for i in range(table.num_total_cols):
                            
                    if i >= START_USER_DATA_COLUMN and schema_encoding[i - START_USER_DATA_COLUMN] == '0':
                        continue
                    else:
                        column = []
                        for j in range(NUMBER_OF_DEXS):
                            val = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                            column.append(val)

                        columns[i] = column

            metarecord = MetaRecord(rid,key,columns)

            if not tail_flag:
                table.key_index[key] = rid
            
            if rid == 0: # Was deleted
                if 0 in table.page_directory:
                    table.page_directory[rid].append(metarecord)
                else:
                    table.page_directory[rid] = [metarecord]
            else:
                table.page_directory[rid] = metarecord

        meta_file.close()
        return table



    ### Writes a page range to disk ###
    # :param pr: pagerange  #Pagerange to write
    # :param pagerange_num: #Number of the pagerange in table
    # :param table_name:    #Name of table that owns pagerange
    # :return bool:         #True if page range was written succesfully
    # :brief    :           #Opens or creates a pagerange file on disk
    #                       #Writes num of base and taile pages in pagerange
    #                       #Then writes the base pages, and then tail pages
    def write_page_range(self, pr, pagerange_num, table_name):
        # print("Here")
        try:
            binary_file = open(self.database_folder + "/" + sanitize(table_name) + "/" + "pagerange_" + str(pagerange_num), "r+b")
        except FileNotFoundError:
            binary_file = open(self.database_folder + "/" + sanitize(table_name) + "/" + "pagerange_" + str(pagerange_num), "w+b")

        # data = encode_pagerange(pagerange)

        # Write Meta
        binary_file.write(int_to_bytes(pr.base_page_count))
        binary_file.write(int_to_bytes(pr.tail_page_count))

        # Write Base Pages
        for i in range(pr.base_page_count):
            page = pr.base_pages[i]
            if page.is_dirty:
                binary_file.write(encode_page(page))  # 8 + PAGE_SIZE
            else:
                binary_file.seek(SIZE_ENCODED_PAGE, 1)

        # Write Tail Pages
        for i in range(pr.tail_page_count):
            page = pr.tail_pages[i]
            if page.is_dirty:
                binary_file.write(encode_page(page))  # 8 + PAGE_SIZE
            else:
                binary_file.seek(SIZE_ENCODED_PAGE, 1)

        binary_file.close()

        return True

    ### Helper function to find start of page data in pagerange file ###
    # :param page_key: tuple    #Tuple of two consisting of (inner page index, pagerange index)
    # :param num_base_pages:    #Number of the base pages in pagerange
    # :returns int:             #Length of need offset from start of pagerange file
    def get_page_offset_in_pr(self, page_key, num_base_pages):
        inner_idx, pagerange_idx = page_key
        
        idx = inner_idx
        if inner_idx >= PAGE_RANGE_MAX_BASE_PAGES:
            idx = num_base_pages
            idx += inner_idx - PAGE_RANGE_MAX_BASE_PAGES

        # Skip meta
        offset = PR_META_OFFSETS[-1]

        # Go to actual offset
        offset += SIZE_ENCODED_PAGE * idx

        return offset


    ### Write page to disk ###
    # :param page:              #Page whose data will be written to disk
    # :param page_key:          #Page_key is a tuple of two (Inner_page_index, pagerange_index)
    # :param table:             #Table the owns the page
    # :param table_folder:      #Table folder of the table
    # :param num_base_pages:    #Number of base pages in pagerange the contains page
    def write_page(self, page, page_key, table, table_folder, num_base_pages = None):
        if page._data == None:
            raise Exception("Page not loaded")
        
        inner_idx, pr_idx = page_key
        filepath = self.database_folder + table_folder + "/" + "pagerange_" + str(pr_idx)
        try:
            binary_file = open(filepath, "r+b")
        except:
            binary_file = open(filepath, "w+b")

        if num_base_pages == None:
            num_base_pages = table.page_ranges[pr_idx].base_page_count

        offset = self.get_page_offset_in_pr(page_key, num_base_pages)
        binary_file.seek(offset)
        
        # What to write

        encoded = encode_page(page)
        binary_file.write(encoded)

        binary_file.close()

    ### Imports a page from disk ###
    # :param page:              #Page whose data will be loaded from disk, if not given makes blank object
    # :param page_key:          #Page_key is a tuple of two (Inner_page_index, pagerange_index)
    # :param table:             #Table the owns the page
    # :param table_folder:      #Table folder of the table
    # :param num_base_pages:    #Number of base pages in pagerange the contains page
    def import_page(self, page: Page, page_key, table, table_folder, num_base_pages = None):
        inner_idx, pr_idx = page_key

        try:
            binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + str(pr_idx), 'rb')
        except FileNotFoundError:
            return False

        if num_base_pages == None:
            num_base_pages = table.page_ranges[pr_idx].base_page_count

        offset = self.get_page_offset_in_pr(page_key, num_base_pages)
        
        # Read num_records
        binary_file.seek(offset)
        num_records = ifb(binary_file.read(8))
        
        # Read data
        data = bytearray(binary_file.read(PAGE_SIZE))

        page.num_records = num_records
        page.load(data, num_records)
        # todo: is_dirty, pinning

        binary_file.close()


    ### Imports pagerange of table ###
    # :param pagerangenum:  #Number of pagerange in table
    # :param table_folder:  #Folder of table that owns pagerange
    # :brief                #Loads pagerange info, but does not load pages
    def import_page_ranges(self, pagerange_num, table_folder):

        try:
            binary_file = open(self.database_folder + sanitize(table_folder) + "/" + "pagerange_" + str(pagerange_num), "r+b")
        except FileNotFoundError:
            raise Exception('Pagerange file not found', sanitize(table_folder), pagerange_num)

        return decode_pagerange(binary_file.read())
        
    # Test stuff
    def rw_test(self):
        og_page = Page()
        og_page.write_tps(999)
        og_page.write(itb(111))
        og_page.write(itb(222))
        og_page.write(itb(333))

        self.write_page(og_page, (0,0), None, 'Grades', 16)

        new_page = Page()
        self.import_page(new_page, (0,0), None, 'Grades', 16)

        results = compare_pages(og_page, new_page)
        print(results)



