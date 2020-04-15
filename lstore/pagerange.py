from config import *
from page import Page
import threading

### PageRange Class ###
# This class manages the pages of that database
# Each pagerange was a max number of base pages (set to 16)
# Base pages are read only and store the original record
# Tail pages append only and contain all updates to records

class PageRange:

    ### Initializer Function ###
    # Class tracks numbewr of base and tail pages
    # As well as a list of size PAGE_RANGE_MAX_BASE_PAGES for base pages
    # And a empty list for tail page
    def __init__(self):
        self.base_pages = [None for _ in range(PAGE_RANGE_MAX_BASE_PAGES)]
        self.tail_pages = []
        self.base_page_count = 0
        self.tail_page_count = 0
        self.tail_page_lock = threading.Lock()

    # Returns true if the number of base pages is less than max base pages
    def has_open_base_pages(self):
        return self.base_page_count < PAGE_RANGE_MAX_BASE_PAGES


    ### Creates new base page ###
    # :returns tuple:       #Tuple of two contaning (page index, new page object)
    def create_base_page(self):

        if self.base_page_count >= PAGE_RANGE_MAX_BASE_PAGES:
            raise Exception('Trying to create base page on full page range')

        inner_page_index = self.base_page_count
        new_page = Page()
        self.base_pages[inner_page_index] = new_page

        self.base_page_count += 1

        return (inner_page_index, new_page)

    ### Gets an open tail page ###
    # :returns tuple:       #TUple of two containing (page_index, existing page object)
    def get_open_tail_page(self):

        with self.tail_page_lock:
            if self.tail_page_count == 0:
                inner_idx, tail_page = self._create_tail_page()
            else:
                inner_idx, tail_page = self._get_latest_tail()
                is_open = tail_page.has_capacity()
                if not is_open:
                    inner_idx, tail_page = self._create_tail_page()

            return (inner_idx, tail_page)

    ### Makes a new tailpage ###
    # :returns tuple:       #Tuple of two containing (inner_page_index, new page object)
    def _create_tail_page(self):
        inner_page_index = self.tail_page_count + PAGE_RANGE_MAX_BASE_PAGES
        new_page = Page()
        self.tail_pages.append(new_page)
        self.tail_page_count += 1

        return (inner_page_index, new_page)

    ### Retrieves the most recent tail page ###
    # :returns tuple:       #Tuple of two containing (inner_page_index, existing page object)
    def _get_latest_tail(self):
        if self.tail_page_count == 0:
            return None

        last_tail_idx = self.tail_page_count - 1
        inner_idx = PAGE_RANGE_MAX_BASE_PAGES + last_tail_idx
        return (inner_idx, self.tail_pages[last_tail_idx])

    ### Retrieves a page ###
    # :param inner_page_index:      #Index of page within pagerange
    # :return page:                 #A base or tail page depending on the index
    def get_page(self, inner_page_index):
        if inner_page_index < PAGE_RANGE_MAX_BASE_PAGES:
            return self.base_pages[inner_page_index]

        last_tp_index = inner_page_index - PAGE_RANGE_MAX_BASE_PAGES
        return self.tail_pages[last_tp_index]