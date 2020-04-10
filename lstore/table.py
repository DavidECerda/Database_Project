import time
import threading
from math import ceil, floor

from page import *
from pagerange import PageRange
from index import Index
from config import *
from util import *
from mergejob import MergeJob
from sxlock import LockManager

# from diskmanager import DiskManager
from bufferpool import BufferPool

import logging

class MetaRecord:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.latch = threading.Lock()

    def copy(self):
        copy = MetaRecord(self.rid, self.key, self.columns)
        return copy

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key_col, disk):
        
        self.name = name
        self.key_col = key_col
        self.num_columns = num_columns
        self.num_sys_columns = 4 # Don't export
        self.num_total_cols = self.num_sys_columns + self.num_columns # Don't export

        self.num_rows = 0

        self.page_ranges = []
        self.page_directory = {}
        self._rw_locks = {} # only base records for now # Don't export
        self._del_locks = {} # Don't export

        self.merge_lock = threading.Lock() # Don't export

        self.prev_rid = 0
        self.rid_latch = threading.Lock()
        self.prev_tid = 2**64 - 1
        self.tid_latch = threading.Lock()

        self.bp = BufferPool(self, disk)
        self.key_index = {} # key -> base MetaRecord PID # Don't export
        self.indices = Index(self) # Don't export
        # self.indices.create_index(key_col)

        self.merging = 0
        self.updates_since_merge = 0

        self.get_open_bp_lock = threading.Lock()
        self.tail_col_lock = threading.Lock()
        self.update_row_lock = threading.Lock()
        self.merge_schedule_lock = threading.Lock()


    def rw_locks(self, rid):
        if rid not in self._rw_locks:
            self._rw_locks[rid] = threading.Lock()

        return self._rw_locks[rid]

    def del_locks(self, rid):
        if rid not in self._del_locks:
            self._del_locks[rid] = threading.Lock()

        return self._del_locks[rid]

    def schedule_merge(self):
        with self.merge_schedule_lock:
            if self.merging <= 0:
                def start_merge():
                    # print('Starting merge')
                    job = MergeJob(self)
                    self.merging = 1
                    job.run()
                    self.merging = 0
                    # print('Merged')

                merge = threading.Thread(target=start_merge, args=())
                merge.start()
                self.updates_since_merge = 0

    def create_index(self, column_idx):
        self.indices.create_index(column_idx)
    
    def drop_index(self, column_idx):
        self.indices.drop_index(column_idx)
    


    def get_page(self, pid): # type: Page
        # cell_idx, page_idx, page_range_idx = pid
        # page_range = self.page_ranges[page_range_idx] # type: PageRange
        # page = page_range.get_page(page_idx) # type: Page

        page = self.bp.get_page(pid, pin=True)
        return page

    def get_page_range(self,page_range_idx):
        return self.page_ranges[page_range_idx]

    def read_pid(self, pid): # type: Page
        page = self.get_page(pid) # type: Page
        read = page.read(pid[0])
        self.bp.unpin((pid[1], pid[2]))
        return read

    def get_open_base_page(self, col_idx, new_row_num):
        # how many pages for this column exists
        num_col_pages = ceil((new_row_num - 1) / CELLS_PER_PAGE)

        # index of the last used page in respect to all pages across all page ranges
        prev_outer_page_idx = col_idx + max(0, num_col_pages - 1) * self.num_total_cols

        # index of last used page range
        prev_page_range_idx = floor(prev_outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)

        # index of last used page in respect to the specific page range
        prev_inner_page_idx = get_inner_index_from_outer_index(prev_outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

        # index of cell within page
        mod = (new_row_num - 1) % CELLS_PER_PAGE
        max_cell_index = CELLS_PER_PAGE - 1
        prev_cell_idx = max_cell_index if (0 == mod) else (mod - 1)

        if max_cell_index == prev_cell_idx: # last page was full
            # Go to next col page

            # New cell's page index in respect to all pages
            outer_page_idx = col_idx if 0 == (new_row_num - 1) else prev_outer_page_idx + self.num_total_cols

            # New cell's page range index
            page_range_idx = floor(outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)
            
            try:
                page_range = self.page_ranges[page_range_idx] # type: PageRange
            except IndexError:
                page_range = PageRange()
                index = len(self.page_ranges)
                self.page_ranges.append(page_range)
                self.bp.write_new_page_range(page_range, index)

            # New cell's page index in respect to pages in page range
            inner_page_idx = get_inner_index_from_outer_index(outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

            cell_idx = 0
            created_inner_page_idx, page = page_range.create_base_page()

            # print("Created new base page")
            if created_inner_page_idx != inner_page_idx:
                raise Exception('Created inner page index is not the same as the expected inner page index',
                    page.get_num_records(),
                    created_inner_page_idx, cell_idx, inner_page_idx, page_range_idx, outer_page_idx)
            
            # base_page_is_new = True

        else: # there's space in the last used page
            outer_page_idx = prev_outer_page_idx
            page_range_idx = prev_page_range_idx
            inner_page_idx = prev_inner_page_idx
            cell_idx = prev_cell_idx + 1
            
            page_range = self.page_ranges[page_range_idx] # type: PageRange

            page = page_range.get_page(inner_page_idx)
            if (None == page):
                raise Exception('No page returned', cell_idx, inner_page_idx, page_range_idx, outer_page_idx, (new_row_num - 1), col_idx)
            
        pid = [cell_idx, inner_page_idx, page_range_idx]

        # if base_page_is_new or not page.is_loaded:
        # print("Trying to add", pid, "to bufferpool")
        self.bp.add_page(pid, page, pin=True)

        return (pid, page)

    def create_row(self, columns_data):

        key = columns_data[self.key_col]

        
        # rids = self.indices.locate(self.key_col, key)

        if key in self.key_index:
        #if rids is not None:
            raise Exception('Key already exists')
            

        # with self.merge_lock:
        # ORDER OF THESE LINES MATTER
        with self.get_open_bp_lock:
            self.num_rows += 1
            indirection_pid, indirection_page = self.get_open_base_page(INDIRECTION_COLUMN, self.num_rows)
            rid_pid, rid_page = self.get_open_base_page(RID_COLUMN, self.num_rows)
            time_pid, time_page = self.get_open_base_page(TIMESTAMP_COLUMN, self.num_rows)
            schema_pid, schema_page = self.get_open_base_page(SCHEMA_ENCODING_COLUMN, self.num_rows)
            column_pids_and_pages = [self.get_open_base_page(START_USER_DATA_COLUMN + i, self.num_rows) for i in range(self.num_columns)]

        # RID
        with self.rid_latch:
            self.prev_rid += 1
            
        rid = self.prev_rid
        rid_in_bytes = int_to_bytes(rid)
        # num_records_in_page = rid_page.write(rid_in_bytes)
        rid_page.write_to_cell(rid_in_bytes, rid_pid[0], increment=True)

        # Indirection
        # indirection_page.write(rid_in_bytes)
        indirection_page.write_to_cell(rid_in_bytes, indirection_pid[0], increment=True)

        # Timestamp
        millisec = int(round(time.time()*1000))
        bytes_to_write = int_to_bytes(millisec)
        # cell_dex = time_page.write(bytes_to_write)
        time_page.write_to_cell(bytes_to_write, time_pid[0], increment=True)

        # Schema Encoding
        schema_encoding = 0
        bytes_to_write = int_to_bytes(schema_encoding)
        # schema_page.write(bytes_to_write)
        schema_page.write_to_cell(bytes_to_write, schema_pid[0], increment=True)
        
        self.bp.unpin((indirection_pid[1], indirection_pid[2]))
        self.bp.unpin((rid_pid[1], rid_pid[2]))
        self.bp.unpin((time_pid[1], time_pid[2]))
        self.bp.unpin((schema_pid[1], schema_pid[2]))

        # User Data
        for i, col_pid_and_page in enumerate(column_pids_and_pages):
            col_pid, col_page = col_pid_and_page
            bytes_to_write = int_to_bytes(columns_data[i])
            # col_page.write(bytes_to_write)
            col_page.write_to_cell(bytes_to_write, col_pid[0], increment=True)
            self.bp.unpin((col_pid[1], col_pid[2]))

            if self.indices.is_indexed(i):
                self.indices.insert(columns_data[i], rid, i)

        sys_cols = [indirection_pid, rid_pid, time_pid, schema_pid]
        data_cols = [pid for pid, _ in column_pids_and_pages]
        record = MetaRecord(rid, key, sys_cols + data_cols)
        self.page_directory[rid] = record

        self._rw_locks[rid] = threading.Lock()
        self._del_locks[rid] = threading.Lock()

        self.key_index[key] = rid
        # self.indices.insert(key, rid, self.key_col)
        return True

    def update_row(self, key, update_data):
        base_rid = self.key_index[key]
        with self.tid_latch:
            self.prev_tid -= 1


        ## Getting data from base record ============
        base_record = self.page_directory[base_rid] # type: MetaRecord

        tail_schema_encoding = 0

        for i,value in enumerate(update_data[::-1]):
            if value is None:
                tail_schema_encoding += 0
            else:
                tail_schema_encoding += 2**i

        if 0 == tail_schema_encoding:

            # Release locks and return
            # release_all(locks)
            return False

        # Get base record indirection
        base_indir_page_pid = base_record.columns[INDIRECTION_COLUMN]
        base_indir_page = self.get_page(base_indir_page_pid) # type: Page
        base_indir_cell_idx, bip, bipr = base_indir_page_pid
        prev_update_rid_bytes = base_indir_page.read(base_indir_cell_idx)
        self.bp.unpin((bip, bipr))

        # Base record encoding
        base_enc_page_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc_page = self.get_page(base_enc_page_pid) # type: Page
        base_enc_cell_idx, bep, bepr = base_enc_page_pid
        self.bp.unpin((bep, bepr))


        ## Meta columns for tail page ==========

        indirection_pid = self.write_tail_column(base_record, INDIRECTION_COLUMN, prev_update_rid_bytes)

        new_rid = self.prev_tid
        rid_in_bytes = int_to_bytes(new_rid)
        rid_pid = self.write_tail_column(base_record, RID_COLUMN, rid_in_bytes)
        
        millisec = int(round(time.time()*1000))
        bytes_to_write = int_to_bytes(millisec)
        time_pid = self.write_tail_column(base_record, TIMESTAMP_COLUMN, bytes_to_write)
        
        bytes_to_write = int_to_bytes(tail_schema_encoding)
        schema_pid = self.write_tail_column(base_record, SCHEMA_ENCODING_COLUMN, bytes_to_write)

        meta_columns = [indirection_pid, rid_pid, time_pid, schema_pid]


        ## Data Columns for tail page ==========
        data_columns = []
        tail_schema_encoding_binary = bin(tail_schema_encoding)[2:].zfill(self.num_columns)

        for i, pid in enumerate(update_data):
            if '0' == tail_schema_encoding_binary[i]:
                data_columns.append(None)
                continue

            col_idx = START_USER_DATA_COLUMN + i
            bytes_to_write = int_to_bytes(update_data[i])
            pid = self.write_tail_column(base_record, col_idx, bytes_to_write)
            data_columns.append(pid)

        ## Create Tail Record

        tail_record = MetaRecord(new_rid, key, meta_columns + data_columns)
        self.page_directory[new_rid] = tail_record


        ## Update base record indirection and schema

        new_rid_bytes = int_to_bytes(new_rid)

        if not base_indir_page.is_loaded:
            base_indir_page = self.get_page(base_indir_page_pid)

        base_indir_page.write_to_cell(new_rid_bytes, base_indir_cell_idx)

        if not base_enc_page.is_loaded:
            base_enc_page = self.get_page(base_enc_page_pid)

        base_schema_enc_bytes = base_enc_page.read(base_enc_cell_idx)
        base_schema_enc_int = int_from_bytes(base_schema_enc_bytes)
        new_base_enc = base_schema_enc_int | tail_schema_encoding
        bytes_to_write = int_to_bytes(new_base_enc)
        base_enc_page.write_to_cell(bytes_to_write, base_enc_cell_idx)

        self.update_indices(tail_schema_encoding_binary, update_data, base_rid)

        return True

    def write_tail_column(self, base_record, column, data):
        with self.tail_col_lock:
            logging.debug("%s: (%s) Start write tail column: %s, data: %s", threading.get_ident(), "write_tail_column", column, data)
            _,_,page_range_idx = base_record.columns[column]
            page_range = self.page_ranges[page_range_idx] # type: PageRange
            column_inner_idx, column_page = page_range.get_open_tail_page()
            column_pid = [None, column_inner_idx, page_range_idx]
            logging.debug("%s: (%s) Got open tail page pid: %s", threading.get_ident(), "write_tail_column", column_pid)
            self.bp.add_page(column_pid, column_page, pin=True)

            # write indirection
            logging.debug("%s: (%s) write tail page pid: %s", threading.get_ident(), "write_tail_column", column_pid)
            num_records_in_page = column_page.write(data)
            column_cell_idx = num_records_in_page - 1
            column_pid[0] = column_cell_idx

            self.bp.unpin((column_inner_idx, page_range_idx))


            return column_pid

    def update_indices(self, tail_schema, update_data, base_rid):
        for col in range(len(update_data)):
            if '1' == tail_schema[col] and self.indices.is_indexed(col):
                self.indices.update_index(base_rid, update_data[col], col)


    def select(self, key, column, query_columns):

        if column == self.key_col:
            try:
                rid = self.key_index[key]
            except KeyError: 
                return []

            # if 0 == self.key_index[key]:
            if 0 == rid:
                # raise Exception("Key has been deleted.")
                return []

            collapsed = self.collapse_row(rid, query_columns)

            record = Record(None, key, collapsed)
            return [record]

        else:
            if not self.indices.is_indexed(column):
                self.indices.create_index(column)

            try:
                rids = self.indices.locate(column, key)
            except:
                return []

            records = []
            query_columns[column] = 0

            for rid in rids:

                collapsed = self.collapse_row(rid, query_columns)
                collapsed[column] = key
                record = Record(None, key, collapsed)
                records.append(record)
            
            return records

    def collapse_row(self, rid, query_columns):
        resp = [None for _ in query_columns]
        # rid = self.key_index[key]
        # rid = self.indices.locate(self.key_col, key)
        need = query_columns.copy()

        lock_attempts = 0
        while(1):

            # Start acquire lock ===========

            lock_attempts += 1
            # acquire_resp = acquire_all([self.merge_lock, self.rw_locks(rid)])
            acquire_resp = acquire_all([self.rw_locks(rid)])
            if acquire_resp is False:
                continue

            locks = acquire_resp
            
            # Acquired lock ===========

            # Reading base record
            base_record = self.page_directory[rid] # type: MetaRecord
            base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
            base_enc_bytes = self.read_pid(base_enc_pid)
            base_enc_binary = bin(int_from_bytes(base_enc_bytes))[2:].zfill(self.num_columns)
            # print(base_enc_pid, "Looking for data using schema",base_enc_binary)
            tps_all = resp.copy()

            for data_col_idx, is_dirty in enumerate(base_enc_binary):
                
                if need[data_col_idx] == 0:
                    continue

                col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                tps = self.get_page(col_pid).read_tps()
                tps_all[data_col_idx] = tps

                data = self.read_pid(col_pid)
                resp[data_col_idx] = int_from_bytes(data)

                if is_dirty == '0':
                    need[data_col_idx] = 0

            # get RID of next tail record
            curr_indir_pid = base_record.columns[INDIRECTION_COLUMN]
            next_rid = int_from_bytes(self.read_pid(curr_indir_pid))
            # read tail records
            while sum(need) != 0 and next_rid < tps: #  todo: or indirection > tps or more?
                curr_record = self.page_directory[next_rid]
                curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
                curr_enc_bytes = self.read_pid(curr_enc_pid)
                curr_enc = int_from_bytes(curr_enc_bytes)
                curr_enc_binary = bin(curr_enc)[2:].zfill(self.num_columns)

                for data_col_idx, is_updated in enumerate(curr_enc_binary):
                    if need[data_col_idx] == 0:
                        continue

                    if is_updated == '0':
                        continue
                    
                    if next_rid >= tps_all[data_col_idx]:
                        need[data_col_idx] = 0
                        continue

                    # print('LOOKED AT TAIL')

                    col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                    data = self.read_pid(col_pid)
                    data = int_from_bytes(data)
                    resp[data_col_idx] = data
                    need[data_col_idx] = 0

                if sum(need) != 0:
                    curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
                    next_rid = int_from_bytes(self.read_pid(curr_indir_pid))

                    if next_rid == rid: # if next rid is base
                        raise Exception("Came back to original, didn't get all we needed")

            # Release locks and return
            release_all(locks)
            return resp

    def delete_record(self, key):

        try:
            base_rid = self.key_index[key]
            #base_rid = self.indices.locate(self.key_col, key)
        except KeyError:
            raise Exception("Not a valid key")

        if 0 == base_rid:
            raise Exception("Key has been deleted.")

        lock_attempts = 0
        while(1):

            # Start acquire lock ===========

            lock_attempts += 1
            # acquire_resp = acquire_all([self.merge_lock, self.rw_locks(base_rid), self.del_locks(base_rid)]) # todo: double check del locks
            acquire_resp = acquire_all([self.rw_locks(base_rid), self.del_locks(base_rid)]) # todo: double check del locks
            if acquire_resp is False:
                continue

            locks = acquire_resp

            # Acquired lock ===========

            base_record = self.page_directory[base_rid]  # type: MetaRecord
            base_rid_page = self.get_page(base_record.columns[RID_COLUMN])
            base_rid_cell_inx,_,_ = base_record.columns[RID_COLUMN]

            base_rid_page.write_to_cell(int_to_bytes(0),base_rid_cell_inx)
            del self.key_index[key]
            # self.indices.remove(self.key_col, key, base_rid)
            if 0 in self.page_directory:
                base_record.rid = 0
                self.page_directory[0].append(base_record)
            else:
                self.page_directory[0] = [base_record]

            base_indir_page_pid = base_record.columns[INDIRECTION_COLUMN]
            new_tail_rid = self.read_pid(base_indir_page_pid)
            new_tail_rid = int_from_bytes(new_tail_rid)


            while True:            
                new_tail_record = self.page_directory[new_tail_rid]
                new_tail_rid_page = self.get_page(new_tail_record.columns[RID_COLUMN]) # type: Page
                new_tail_rid_cell_inx,_,_ = new_tail_record.columns[RID_COLUMN]

                new_tail_rid_page.write_to_cell(int_to_bytes(0),new_tail_rid_cell_inx)
                del self.page_directory[new_tail_rid]
                self.page_directory[0].append(new_tail_record)
                if(base_rid == new_tail_rid):
                    break
                else:
                    new_tail_indir_page_pid = new_tail_record.columns[INDIRECTION_COLUMN]
                    new_tail_rid = self.read_pid(new_tail_indir_page_pid)
                    new_tail_rid = int_from_bytes(new_tail_rid)

            # Release locks and return
            for i in range(len(self.indices.indices)):
                if self.indices.is_indexed(i):
                    self.indices.remove_by_rid(i, base_rid)

            release_all(locks)
            return True

        del self._del_locks[base_rid]
        del self._rw_locks[base_rid]


    def sum_records(self, start_range, end_range, aggregate_column_index):
        query_columns = [0]*self.num_columns
        query_columns [aggregate_column_index] = 1

        sum = 0
            
        if start_range <= end_range:
            curr_key = start_range
            end = end_range
        else:
            curr_key = end_range
            end = start_range


        while curr_key != (end+1): 

            try:
                curr_rid = self.key_index[curr_key]
                # curr_rid = self.indices.locate(self.key_col, curr_key)
            except KeyError:
                curr_key += 1
                continue

            if curr_rid == 0:
                curr_key += 1 
                continue

            value = self.collapse_row(curr_rid, query_columns)[aggregate_column_index]

            sum += value
            curr_key += 1

        return sum

