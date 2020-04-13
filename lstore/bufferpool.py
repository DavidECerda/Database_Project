from contextlib import contextmanager
none_context = contextmanager(lambda: iter([None]))

from page import Page
from pagerange import PageRange
from config import *
from collections import defaultdict

import logging
import threading
from queue import Queue

### BufferPool class handles which pages are in memory and on disk
class BufferPool:
    ### Initializer for bufferpool ###
    # :param disk:          #Diskmanager class that handles actually reading and writing to disk
    # :param table:         #Table class whose pages the bufferpool will manage
    # :IV self.pins:        #Pins represent queries using the pages
    # :IV self.pages:       #Manages pages in a list.
    # :IV self.merge_pins   #Merge pins keep track whether pages are within a merge job
    def __init__(self, table, disk):
        self.max_pages = MAX_POOL_PAGES # from config file
        self.disk = disk # type : DiskManager
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        self.merge_pins = defaultdict(int)
        self.pins = defaultdict(int)
        self.page_index = {}
        self.loaded_off_pool = []

        self.table = table # type: Table

        self.pop_locks = [threading.Lock() for _ in range(500)]
        self.load_locks = [threading.Lock() for _ in range(500)]

        self.add_queue = Queue()

        threading.Thread(target=self._handle_add_pool).start()

    ### Internal function to handle adding pages to the bufferpool ###
    # :brief    :           #Gets which pages to add from the add_queue
    #                       #Calls pop_pages to remove pages if bufferpool is full.
    def _handle_add_pool(self):
        while True:
            pool_key = self.add_queue.get()
            page_key, _ = pool_key

            logging.debug("%s: (%s) start: %s", threading.get_ident(), "_update_pool_get", page_key)
            try:
                idx = self.pages.index(pool_key)
                self.pages.pop(idx)
            except ValueError:
                logging.debug("%s: (%s) page not in pool: %s", threading.get_ident(), "_update_pool_get", page_key)
                self.num_pool_pages += 1

            logging.debug("%s: (%s) adding to bufferpool pagekey: %s", threading.get_ident(), "_update_pool_get", page_key)
            self.pages.append(pool_key)
            logging.debug("%s: (%s) added to bufferpool pagekey: %s", threading.get_ident(), "_update_pool_get", page_key)

            if self.num_pool_pages > MAX_POOL_PAGES:
                self._pop_pages()

    ### Updates which pages need to be added ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    # :param page:          #Page to be added
    def _update_pool_get(self, page_key, page):
        pool_key = (page_key, page)
        self.add_queue.put(pool_key)

    ### Gets a page ###
    # :param pid:           #Consists of a tuple in the form of (inner_page_idx, page_idx, page_range_idx)
    # :param pin:           #If the the page is to be pinned or not
    # :brief    :           #Creates a lock on the page with hash function
    #                       #Will create a lock and call self.add_page
    # :return   :           #Page that was retrieved

    def get_page(self, pid, pin=False):
        _, page_idx, page_range_idx = pid
        page_range = self.table.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page
        page_key = (pid[1], pid[2])

        hashed = self.hash(page_key, len(self.load_locks))
        lock = self.pop_locks[hashed]

        with lock:
            logging.debug("%s: (%s) start: %s", threading.get_ident(), "get_page", pid)
            
            if pin:
                self.pin(page_key)

            self.add_page(pid, page, have_lock=True)
            
            return page

    ### Adds a page to the bufferpool ###
    # :param pid:           #Consists of a tuple in the form of (inner_page_idx, page_idx, page_range_idx)
    # :param pin:           #Flag if the page is to be pinned or not
    # :param lock:          #Flag if the page is locked
    # :brief    :           #Creates a lock on the page if have_lock=False
    #                       #Will call self._load_from_disk if page is not loaded in memory
    #                       #Calls self._update_pool_get to update which pages to add to bufferpool

    def add_page(self, pid, page, pin=False, have_lock=False):
        logging.debug("%s: (%s) start pid: %s", threading.get_ident(), "add_page", pid)
        
        page_key = (pid[1], pid[2])
        logging.debug("%s: (%s) start: %s", threading.get_ident(), "get_page", pid)
        
        if have_lock:
            lock = None
        else:
            hashed = self.hash(page_key, len(self.load_locks))
            lock = self.pop_locks[hashed]

        with (lock if lock is not None else none_context()):
            if pin:
                self.pin(page_key)

            if not page.is_loaded:
                logging.debug("%s: (%s) load from disk pid: %s", threading.get_ident(), "get_page", pid)
                self._load_from_disk(page_key, page)

            self._update_pool_get(page_key, page)

    ### Removes pages from bufferfool ###
    # :brief    :           #Removes the least recently used (LRU) pages from bufferpool
    #                       #Will remove a quarter of the bufferpool at a time for faster page refresh times
    #                       #Marks pages with 0 pins as good for removal
    #                       #For each page that is removed, will lock before removal and
    #                       #Checks if the page was possibly pinned in another thread or is in a mergejob
    #                       #Calls function to write to disk if page is dirty
    #                       #Unloads the page to be removes through page class function

    def _pop_pages(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        logging.debug("{}: {}".format(threading.get_ident(), "bp._pop_page"))

        i = 0

        pages_to_remove = []
        num_pages_to_remove = self.num_pool_pages//4

        while i < self.num_pool_pages and len(pages_to_remove) < num_pages_to_remove:

            page_key, page_to_pop = self.pages[i]

            if self.pins[page_key] <= 0:
                pages_to_remove.append(self.pages.pop(i))
                self.num_pool_pages -= 1

            if i >= self.num_pool_pages:
                i = 0
            else:
                i += 1

        for page in pages_to_remove:

            page_key, page_to_pop = page
            hashed = self.hash(page_key, len(self.load_locks))
            lock = self.pop_locks[hashed]
            with lock:
                if self.pins[page] > 0:
                    raise Exception("Trying to remove page that is pinned")

                if self.merge_pins[page_key] == 1:
                    logging.debug("%s: (%s) wanted to unload page pid: %s but ", threading.get_ident(), "_pop_page", page_key)
                    self.loaded_off_pool.append((page_key, page_to_pop))
                else:
                    if page_to_pop.is_dirty:
                        self._write_to_disk(page_key, page_to_pop)

                    logging.debug("%s: (%s) unloading page pid: %s", threading.get_ident(), "_pop_page", page_key)
                    page_to_pop.unload()
                    logging.debug("%s: (%s) unloaded page pid: %s", threading.get_ident(), "_pop_page", page_key)

    ### Calls on diskmanager to create a new pagerange in disk ###
    # :param page_range:    #new page_range class with to write to disk
    # :param num:           #page_range number       
    def write_new_page_range(self, page_range, num):
        self.disk.write_page_range(page_range, num, self.table.name)

    ### Creates a hash for a page ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    def hash(self, page_key, num):
        to_hash = page_key[0] + (100 * (page_key[1] +1))
        hashed = to_hash % num
        return hashed

    ### Calls on diskmanager to load page from disk ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    # :param page:          #Page to be added
    def _load_from_disk(self, page_key, page):
        hashed = self.hash(page_key, len(self.load_locks))
        lock = self.load_locks[hashed]

        with lock:
            if not page.is_loaded:
                self.disk.import_page(page, page_key, self.table, self.table.name)
                
            return page
    
    ### Calls on diskmanager to write page to disk ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    # :param page:          #Page to be added
    def _write_to_disk(self, page_key, page):
        self.disk.write_page(page, page_key, self.table, self.table.name)
        page.is_dirty = False

    ### Adds a pin to a page ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    def pin(self, page_key):
        self.pins[page_key] += 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.pin", page_key, self.pins[page_key]))

    ### Removes a pin to a page ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    def unpin(self, page_key):
        if self.pins[page_key] != 0:
            self.pins[page_key] -= 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.unpin", page_key, self.pins[page_key]))

    ### Adds a merge pin to a page ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    def pin_merge(self, page_key):
        logging.debug("%s: %s %s", threading.get_ident(), "start bp.pin_merge", page_key)

        if page_key in self.merge_pins:
            self.merge_pins[page_key] += 1
        else:
            self.merge_pins[page_key] = 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.pin_merge", page_key, self.merge_pins[page_key]))

    ### Removes a merge pin to a page ###
    # :param page_key:      #Consists of a tuple in the form of (page_idx, page_range_idx)
    def unpin_merge(self, page_key):
        logging.debug("%s: %s %s", threading.get_ident(), "start bp.unpin_merge", page_key)
        if self.merge_pins[page_key] != 0:
            self.merge_pins[page_key] -= 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "end bp.unpin_merge", page_key, self.merge_pins[page_key]))

    ### Unloads pages that were in a mergejob ###
    # :brief    :       #All pages marked for removal by _pop_pages that couldn't be removed since they were being merged
                        #Are removed if the are not pinned and written to disk if dirty
    def flush_unpooled(self):
        logging.debug("%s: %s", threading.get_ident(), "start bp.flush_unpooled")
        while self.loaded_off_pool:
            page_key, page_to_pop = self.loaded_off_pool.pop()
            logging.debug("%s: %s %s", threading.get_ident(), "considering bp.flush_unpooled", page_key)

            if self.pins[page_key] > 0:
                logging.debug("%s: Was about to flush a page that was pinned", threading.get_ident())
                continue

            with page_to_pop.latch:
                if page_to_pop.is_dirty:
                    self._write_to_disk(page_key, page_to_pop)

                page_to_pop.unload()
                if (page_key, page_to_pop) in self.pages:
                    raise Exception("%s: (%s) flushed page that is in bufferpool", threading.get_ident(), "flush_unpooled")

                logging.debug("{}: {} {} {} {}".format(threading.get_ident(), "bp.flush_unpooled unloaded item", page_key, self.pins[page_key], self.pins[page_key]))