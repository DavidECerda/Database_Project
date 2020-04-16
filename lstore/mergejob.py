from table import *
from config import *
from page import Page
from util import *


### MergeJob Class ###
# Class handles the innerworks of a merge
# A merge will take all the records of a table and
# Update the base record values based on the existing tail records
class MergeJob:

    ### Initializer function of the mergejob ###
    # :param table:     #Table whose records will be merged
    def __init__(self, table):
        self.copied_metarecords = {} # key to metarecord
        self.copied_base_pages = {} # (inner, pr) to page
        self.copied_prev_rid = None
        self.min_tid = RESERVED_TID
        self.table = table # type: Table
        self.to_unpin = []

    ### Function that copies all the base records ###
    # :returns copies:  #List of copied records
    # :brief    :       #Copies all the base records and metarecords and uses those for merge
    def copy_data(self):
        self.copied_prev_rid = self.table.prev_rid
        copied_metarecords = {}
        copied_base_pages = {}
        for rid in range(1, self.copied_prev_rid+1):
            if rid not in self.table.page_directory:
                continue

            current_record = self.table.page_directory[rid].copy()
        
            for pid in current_record.columns:
                cell_idx, inner_idx, range_idx = pid

                page_key = (inner_idx, range_idx)
                if page_key not in copied_base_pages:
                    og_page = self.table.get_page(pid)

                    self.table.bp.pin_merge(page_key)
                    if page_key not in self.to_unpin:
                        self.to_unpin.append(page_key)

                    copied_base_pages[(inner_idx, range_idx)] = og_page.copy()
                    
                
            copied_metarecords[rid] = current_record

        return [copied_metarecords, copied_base_pages]
        
    ### Reads the copied pages ###
    # :param pid: tuple     #Tuple of three (cell_idx, inner_idx, range_idx) that allows reading a single cell
    # returns bytes:        #Returns the bytes in the cell specified by the pid
    def read_copied_by_pid(self, pid):
        cell_idx, inner_idx, range_idx = pid
        page = self.copied_base_pages[(inner_idx, range_idx)] # type: Page
        bytes_read = page.read(cell_idx)
        return bytes_read

    # Same as above but returns the int values
    def read_copied_by_pid_as_int(self, pid):
        return int_from_bytes(self.read_copied_by_pid(pid))


    
    ### Function to write to copied page ###
    # :param pid:       #full address, see read_copied_by_pid
    # :param data: int  #Data as an int
    def write_to_copied_by_pid(self, pid, data):

        cell_idx, inner_idx, range_idx = pid
        page = self.copied_base_pages[(inner_idx, range_idx)] # type: Page
        bytes_to_write = int_to_bytes(data)
        page.write_to_cell(bytes_to_write, cell_idx)
        self.copied_base_pages[(inner_idx, range_idx)] = page

    # Function to get latest record values, see collapse_record from table.py
    def collapse_record(self, rid):
        base_record = self.copied_metarecords[rid] # type: MetaRecord

        resp = [None for _ in range(self.table.num_columns)]
        need = [1 for _ in range(self.table.num_columns)]

        base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc = self.read_copied_by_pid_as_int(base_enc_pid)
        base_enc_binary = bin(base_enc)[2:].zfill(self.table.num_columns)

        tps_all = resp.copy()

        for data_col_idx, is_dirty in enumerate(base_enc_binary):
            if need[data_col_idx] == 0:
                continue
            col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
            tps = self.table.get_page(col_pid).read_tps()
            tps_all[data_col_idx] = tps

            data = self.read_copied_by_pid(col_pid)
            resp[data_col_idx] = int_from_bytes(data)

            if is_dirty == '0':
                need[data_col_idx] = 0

        # get RID of next tail record
        curr_indir_pid = base_record.columns[INDIRECTION_COLUMN]
        next_rid = self.read_copied_by_pid_as_int(curr_indir_pid)

        # read tail records
        while sum(need) != 0: #  todo: or indirection > tps or more?
            self.min_tid = min(self.min_tid, next_rid)
            curr_record = self.table.page_directory[next_rid]

            curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
            curr_enc_bytes = self.table.read_pid(curr_enc_pid)
            curr_enc = int_from_bytes(curr_enc_bytes)
            curr_enc_binary = bin(curr_enc)[2:].zfill(self.table.num_columns)

            for data_col_idx, is_updated in enumerate(curr_enc_binary):
                if need[data_col_idx] == 0:
                    continue

                if is_updated == '0':
                    continue
                
                if next_rid >= tps_all[data_col_idx]:
                    need[data_col_idx] = 0
                    continue

                col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                data = self.table.read_pid(col_pid)
                data = int_from_bytes(data)
                resp[data_col_idx] = data
                need[data_col_idx] = 0

            if sum(need) != 0:
                curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
                next_rid = int_from_bytes(self.table.read_pid(curr_indir_pid))

                if next_rid == rid: # if next rid is base
                    raise Exception("Came back to original, didn't get all we needed")

        return [base_record, resp]

    ### Writes merged data to copied pages ###
    # :param base_record:       #Base record whose data has been merged
    # :param data_cols:         #Data values from the merge
    def write_collapsed_pages(self, base_record, data_cols):
        
        for i, data in enumerate(data_cols):
            pid = base_record.columns[START_USER_DATA_COLUMN + i]
            self.write_to_copied_by_pid(pid, data)

    # Writes the tail processing sequence number to all the pages
    def write_tps_to_all(self):

        for page in self.copied_base_pages.values():
            page.write_tps(self.min_tid)

    ### Loads merged pages back into table ###
    # :param merged_record:     #Merged record with the new merged data
    # :brief            :       #Loads the merged pages into the table, overriding the original pages
    #                           #Will check if new records have been added and copies those to the merged pages
    def load_into_table(self, merged_record):
        og_record = self.table.page_directory[merged_record.rid]
        merged_data_cols = merged_record.columns[START_USER_DATA_COLUMN:]

        for i, pid in enumerate(merged_data_cols):
            cell_idx, inner_idx, range_idx = pid
            page_key = (inner_idx, range_idx)
            new_page = self.copied_base_pages[page_key]

            og_page = self.table.get_page(pid)

            # Make sure the page is loaded first
            with og_page.latch:
                if not og_page.is_loaded:
                    raise Exception("The original page isn't loaded")
                data = new_page._data
                if og_page.num_records != new_page.num_records:
                    idx = new_page.num_records # also accounts for tps
                    data = data[:idx] + og_page._data[idx:]

                original_data = og_page._data
                og_page._data = data
                og_page.is_dirty = True
                og_page.is_loaded = True

    ### Runs the merge ###
    # :brief    :       #Copies the data and merges it with the talk records
    #                   #Then reloads it back into the table, while managing all the locks needed
    def run(self):

        self.table.merging = 1
        
        with self.table.merge_lock:
            self.copied_metarecords, self.copied_base_pages = self.copy_data()

        self.table.merging = 2

        for rid in range(1, self.copied_prev_rid+1):

            if rid not in self.table.page_directory:
                continue

            # Lock record from deletion
            self.table.del_locks(rid).acquire()
            if rid not in self.table.page_directory: # If we just acquired the del lock, we might've acquired it after a delete
                self.table.del_locks(rid).release()
                # print('Got lock but the base record was deleted')
                continue

            self.table.merging = 3
            # Collapse
            base_record, data_cols = self.collapse_record(rid)
            self.write_collapsed_pages(base_record, data_cols)

            # Release lock
            self.table.del_locks(rid).release()

        self.table.merging = 4
        self.write_tps_to_all()

        for rid in range(1, self.copied_prev_rid+1):

            if rid not in self.copied_metarecords:
                continue

            # Lock record from rw/deletion
            while(1):
                acquire_resp = acquire_all([self.table.rw_locks(rid), self.table.del_locks(rid)])
                if acquire_resp is False:
                    continue
                locks = acquire_resp

                if rid not in self.table.page_directory: # If we just acquired the del lock, we might've acquired it after a delete
                    release_all(locks)
                    break

                self.table.merging = 4
                # Write back
                metarecord = self.copied_metarecords[rid]
                self.load_into_table(metarecord)

                # Release lock
                release_all(locks)
                break

        self.table.merging = 5
        for page_key in self.to_unpin:
            self.table.bp.unpin_merge(page_key)

        with self.table.merge_lock:
            self.table.bp.flush_unpooled()

        self.table.merging = 0