from config import *
from math import ceil, floor
from util import *
from page import *
from pagerange import PageRange

class DataManager:

    def __init__(self, table):
        self.page_ranges = []
        self.table = table

    def test(self):
        print('test')
        pass

    def get_open_base_page(self, col_idx):
        # how many pages for this column exists
        num_col_pages = ceil(self.table.num_rows / CELLS_PER_PAGE)

        # index of the last used page in respect to all pages across all page ranges
        prev_outer_page_idx = col_idx + max(0, num_col_pages - 1) * self.table.num_total_cols

        # index of last used page range
        prev_page_range_idx = floor(prev_outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)

        # index of last used page in respect to the specific page range
        prev_inner_page_idx = get_inner_index_from_outer_index(prev_outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

        # index of cell within page
        mod = self.table.num_rows % CELLS_PER_PAGE
        max_cell_index = CELLS_PER_PAGE - 1
        prev_cell_idx = max_cell_index if (0 == mod) else (mod - 1)

        if max_cell_index == prev_cell_idx: # last page was full
            # Go to next col page

            # New cell's page index in respect to all pages
            outer_page_idx = col_idx if 0 == self.table.num_rows else prev_outer_page_idx + self.table.num_total_cols

            # New cell's page range index
            page_range_idx = floor(outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)
            
            try:
                page_range = self.page_ranges[page_range_idx] # type: PageRange
            except IndexError:
                page_range = PageRange()
                self.page_ranges.append(page_range)

            # New cell's page index in respect to pages in page range
            inner_page_idx = get_inner_index_from_outer_index(outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

            cell_idx = 0
            created_inner_page_idx, page = page_range.create_base_page()

            if created_inner_page_idx != inner_page_idx:
                raise Exception('Created inner page index is not the same as the expected inner page index',
                    page.get_num_records(),
                    created_inner_page_idx, cell_idx, inner_page_idx, page_range_idx, outer_page_idx)

        else: # there's space in the last used page
            outer_page_idx = prev_outer_page_idx
            page_range_idx = prev_page_range_idx
            inner_page_idx = prev_inner_page_idx
            cell_idx = prev_cell_idx + 1
            
            page_range = self.page_ranges[page_range_idx] # type: PageRange

            page = page_range.get_page(inner_page_idx)
            if (None == page):
                raise Exception('No page returned', cell_idx, inner_page_idx, page_range_idx, outer_page_idx, self.table.num_rows, col_idx)
            
        pid = [cell_idx, inner_page_idx, page_range_idx]  
        return (pid, page)

    def get_page(self, pid): # type: Page
        cell_idx, page_idx, page_range_idx = pid
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page

        return page

    def read_pid(self, pid): # type: Page
        cell_idx, page_idx, page_range_idx = pid
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page
        read = page.read(cell_idx)

        return read

    def read_metarecord(self, metarecord, column):
        pid = metarecord.columns[column]
        page = self.get_page(pid) # type: Page
        cell_idx, _, _ = pid
        read_bytes = page.read(cell_idx)
        return read_bytes

    def write_metarecord(self, metarecord, column, write):
        pid = metarecord.columns[column]
        cell_idx, _, _ = pid

        bytes_to_write = int_to_bytes(write)
        page = self.get_page(pid) # type: Page
        page.writeToCell(bytes_to_write, cell_idx)