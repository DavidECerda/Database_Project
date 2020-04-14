from config import *
from util import *
from time import process_time

import threading

### Page object ###
# Represents a page of data (set to 4096 bytes) as a byte array
# Keeps track of number of records and whether the it has been loaded or written to
# Also, each page was a 8 bytes reserved for a tail processing sequence number (tps)
class Page:

    ### Initializer funtion  ###
    # :param is_importing: bool     #Determines whether the page will be loaded with data from disk or be a new blank page. 
    def __init__(self, is_importing = False):
        self.num_records = 0
        self.num_records_lock = threading.Lock()
        # self.indexes = None
        # self.write_lock = threading.Lock()
        # self.latch = pl.get(process_time())
        self.latch = threading.Lock()

        if is_importing:
            self._data = None
            self.is_loaded = False
            self.is_dirty = False
        else:
            self._data = bytearray(PAGE_SIZE)
            self.is_loaded = True
            self.write_tps(RESERVED_TID)
            self.is_dirty = True

    @property
    def is_loaded(self):
        return self.__is_loaded

    @is_loaded.setter
    def is_loaded(self, is_loaded):
        self.__is_loaded = is_loaded

    @property
    def is_dirty(self):
        return self.__is_loaded

    @is_dirty.setter
    def is_dirty(self, is_loaded):
        self.__is_loaded = is_loaded
        

    ### Loads data ###
    # :param data: byte array       #Data to be loading onto the page
    # :param num_records:           #Number of records in the page
    # :param is_dirty:              #If the page needs to be written to disk
    # :param force:                 #Forces preexisting page data to be over written
    def load(self, data, num_records=None, is_dirty=None, force=False):
        if self.is_loaded and not force:
            return self

        self.is_loaded = True
        self._data = data

        if num_records is not None:
            self.num_records = num_records

        if is_dirty is not None:
            self.is_dirty = is_dirty

        return self
    # Sets data to none and is_loaded to false
    def unload(self):
        self._data = None
        self.is_loaded = False
    
    def has_capacity(self):
        return self.num_records < (CELLS_PER_PAGE)

    def get_num_records(self):
        return self.num_records

        
    ### Writes bytes to the page ###
    # :param value: bytes       #Dat  to write. Must be bytes of the specified CELLS_PER_PAGE size
    # :brief    :               #Writes the bytes to the next available cell in the page
    #                           #Which is determined by the number of records in page
    #                           #Marks page as dirty 
    # return int:               #Returns the number of records in the page   
    def write(self, value):
        with self.num_records_lock:
            if not self.has_capacity():
                raise Exception('page is full')

            start = (self.num_records + 1) * CELL_SIZE_BYTES
            self.num_records += 1
            record_num = self.num_records # the number of this particular record (index+1)

        end = start + CELL_SIZE_BYTES
        if len(value) != CELL_SIZE_BYTES:
            value = int.from_bytes(value, 'little')
            value = value.to_bytes(CELL_SIZE_BYTES, 'little')
        self._data[start:end] = value
        self.is_dirty = True
        return record_num

    
    ### Writes bytes to the page at specific cell ###
    # :param value: bytes       #Data to write. Must be bytes of the specified CELLS_PER_PAGE size
    # :param cell_idx: int      #Index of cell to write to. Only write to what has already been written too
    # :param increment:         #If writing to cell adds another record to page
    # :brief    :               #Allows writing to preexisting cells and updating data
    # :return int:              #Returns number of records in the page
    def write_to_cell(self, value, cell_idx, increment=False):

        if increment:
            if not self.has_capacity():
                raise Exception('page is full')
            self.num_records += 1

        start = (cell_idx + 1) * CELL_SIZE_BYTES
        end = start + CELL_SIZE_BYTES
        if len(value) != CELL_SIZE_BYTES:
            value = int.from_bytes(value,'little')
            value = value.to_bytes(CELL_SIZE_BYTES,'little')
        self._data[start:end] = value
        self.is_dirty = True

        return self.num_records

    ### Writes tail page sequencing number ###
    # :param tid: int       :Last tail id the was merging into the base record
    def write_tps(self, tid):
        bytes_to_write = tid.to_bytes(CELL_SIZE_BYTES,'little')
        start = 0
        end = start + CELL_SIZE_BYTES
        self._data[start:end] = bytes_to_write
        self.is_dirty = True

    def read_tps(self) -> int:
        return int_from_bytes(bytes(self._data[0:CELL_SIZE_BYTES]))

    ### Reads a specific cell ###
    # :param cellIndex: int     #Index of cell to read from
    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''
        if cellIndex > CELLS_PER_PAGE - 1:
            raise Exception('cellIndex exceeds page size')

        start = (cellIndex + 1) * CELL_SIZE_BYTES
        end = start + CELL_SIZE_BYTES
        return bytes(self._data[start:end])

    # Copies page
    def copy(self):
        copy = Page()
        copy._data = self._data.copy()
        copy.num_records = self.num_records
        return copy

