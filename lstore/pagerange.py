from config import *
from page import Page
import threading

  e
class PageRange:

    def __init__(self):
        self.base_pages = [None for _ in range(PAGE_RANGE_MAX_BASE_PAGES)]
        self.tail_pages = []
        self.base_page_count = 0
        self.tail_page_count = 0
        self.tail_page_lock = threading.Lock()


    def has_open_base_pages(self):
        return self.base_page_count < PAGE_RANGE_MAX_BASE_PAGES

    def create_base_page(self):
        '''
            Creates base page and returns (page index, page instance)
        '''

        if self.base_page_count >= PAGE_RANGE_MAX_BASE_PAGES:
            raise Exception('Trying to create base page on full page range')

        inner_page_index = self.base_page_count
        new_page = Page()
        self.base_pages[inner_page_index] = new_page

        self.base_page_count += 1

        return (inner_page_index, new_page)

    def get_open_tail_page(self):
        '''
            Returns (inner_page_index, tail_page)
        '''
        with self.tail_page_lock:
            if self.tail_page_count == 0:
                inner_idx, tail_page = self._create_tail_page()
            else:
                inner_idx, tail_page = self._get_latest_tail()
                is_open = tail_page.has_capacity()
                if not is_open:
                    inner_idx, tail_page = self._create_tail_page()

            return (inner_idx, tail_page)

    def _create_tail_page(self):
        '''
            Returns (inner_page_index, new_page)
        '''
        inner_page_index = self.tail_page_count + PAGE_RANGE_MAX_BASE_PAGES
        new_page = Page()
        self.tail_pages.append(new_page)
        self.tail_page_count += 1

        return (inner_page_index, new_page)

    def _get_latest_tail(self):
        '''
            Returns (inner_page_index, latest_tail_page)
        '''
        if self.tail_page_count == 0:
            return None

        last_tail_idx = self.tail_page_count - 1
        inner_idx = PAGE_RANGE_MAX_BASE_PAGES + last_tail_idx
        return (inner_idx, self.tail_pages[last_tail_idx])

    def get_page(self, inner_page_index):
        if inner_page_index < PAGE_RANGE_MAX_BASE_PAGES:
            return self.base_pages[inner_page_index]

        last_tp_index = inner_page_index - PAGE_RANGE_MAX_BASE_PAGES
        return self.tail_pages[last_tp_index]