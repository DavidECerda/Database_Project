from page import Page
from pagerange import PageRange
from util import *
from config import *

def encode_pagerange(pr):

    #
    # Write Meta
    #
    BYTES_base_page_count = int_to_bytes(pr.base_page_count)
    BYTES_tail_page_count = int_to_bytes(pr.tail_page_count)

    #
    # Write Base Pages
    #

    BYTES_base_pages = b''
    for i in range(pr.base_page_count):
        page = pr.base_pages[i]
        BYTES_base_pages += encode_page(page)  # 8 + PAGE_SIZE

    #
    # Write Tail Pages
    #

    BYTES_tail_pages = b''
    for i in range(pr.tail_page_count):
        page = pr.tail_pages[i]
        BYTES_tail_pages += encode_page(page)

    write = b''
    write += BYTES_base_page_count  # 8 bytes
    write += BYTES_tail_page_count  # 8 bytes
    write += BYTES_base_pages      # num_bp * (8+PAGE_SIZE)
    write += BYTES_tail_pages      # num_tp * (8+PAGE_SIZE)

    return write

def encode_page(page: Page):
    if page == None:
        num_records = 0
        data = bytearray(PAGE_SIZE)
    else:
        num_records = page.num_records
        data = page._data

    out = b''
    out += int_to_bytes(num_records)  # 8
    out += bytearray(data)            # PAGE_SIZE

    return out

def decode_pagerange(BYTES_pr) -> PageRange:

    pr = PageRange()

    #
    # Read Meta
    #

    BYTES_base_page_count = BYTES_pr[PR_META_OFFSETS[0]:PR_META_OFFSETS[1]]  # 8 bytes
    BYTES_tail_page_count = BYTES_pr[PR_META_OFFSETS[1]:PR_META_OFFSETS[2]]  # 8 bytes

    pr.base_page_count = int_from_bytes(BYTES_base_page_count)
    pr.tail_page_count = int_from_bytes(BYTES_tail_page_count)

    #
    # Read Base Pages
    #

    bytes_page_size = 8 + PAGE_SIZE
    offset_end_base = 16

    for i in range(pr.base_page_count):
        start = PR_META_OFFSETS[-1] + (i * bytes_page_size)
        end = start + bytes_page_size
        offset_end_base = end  # so we know where the tails start
        BYTES_page = BYTES_pr[start:end]

        page = decode_page(BYTES_page)
        pr.base_pages[i] = page


    #
    # Read Tail Pages
    #

    for i in range(pr.tail_page_count):
        start = offset_end_base + (i * bytes_page_size)
        end = start + bytes_page_size
        BYTES_page = BYTES_pr[start:end]

        page = decode_page(BYTES_page)
        pr.tail_pages.append(page)

    return pr

def decode_page(BYTES_page) -> Page:
    page = Page(True) # type: Page
    # data, num_records=None, is_dirty=None, force=False):
    # is_loaded = True
    num_records = int_from_bytes(BYTES_page[0:8])
    data = bytearray(BYTES_page[8:])
    page.load(data, num_records)
    return page

def fetch_pr_bytes(pr_idx):
    pass

def extract_page_data_from_pr(pr_bytes, inner_idx):

    max_base_pages = 16
    offset_start_bp = 16
    bytes_page_size = 8 + PAGE_SIZE
    
    if inner_idx < 16: # Getting base page
        pass
    else:
        pass

def compare_pages(a, b):
    results = []
    test = a.num_records == b.num_records
    results.append(test)
    # print('\tNum Records match', test)

    test = a.read_tps() == b.read_tps()
    results.append(test)
    # print('\tTPS match', test)

    for r in range(a.num_records):
        test = int_from_bytes(a.read(r)) == int_from_bytes(b.read(r))
        results.append(test)
        # print('\tRecord', i, 'match', test)

    return results
    # print('')

def compare_page_ranges(a, b):
    results = []

    test = b.base_page_count == a.base_page_count
    results.append(test)
    # print('Base pages count match:', test)

    test = b.tail_page_count == a.tail_page_count
    results.append(test)
    # print('Tail pages count match:', test)

    # print('Comparing base pages...')
    for i, page in enumerate(b.base_pages):
        # print('\tPage', i)
        if page == None:
            test = a.base_pages[i] == None
            results.append(test)
            # print('\tNone match', test)
            continue

        test = page.num_records == a.base_pages[i].num_records
        results.append(test)
        # print('\tNum Records match', test)

        test = page.read_tps() == a.base_pages[i].read_tps()
        results.append(test)
        # print('\tTPS match', test)

        for r in range(page.num_records):
            og_page = a.base_pages[i]
            test = int_from_bytes(page.read(r)) == int_from_bytes(og_page.read(r))
            results.append(test)
            # print('\tRecord', i, 'match', test)

        # print('')

    # print('\nComparing tail pages...')
    for i, page in enumerate(b.tail_pages):
        # print('\tPage', i)
        # print('\tNum Records match', page.num_records == a.tail_pages[i].num_records)
        # print('\tTPS match', page.read_tps() == a.tail_pages[i].read_tps())

        for r in range(page.num_records):
            og_page = a.tail_pages[i]
            test = int_from_bytes(page.read(r)) == int_from_bytes(og_page.read(r))
            results.append(test)
            # print('\tRecord', i, 'match', test)

        # print('')
    
    # print('Done')
    return results

def test():

    pr = PageRange()
    pr.base_page_count = 6
    pr.tail_page_count = 2

    def _create_page(i):
        page = Page()
        page.write_tps(666+i)
        page.write(int_to_bytes(777+i))
        page.write(int_to_bytes(888+i))
        page.write(int_to_bytes(999+i))
        return page

    pr.base_pages[0] = _create_page(0)
    pr.base_pages[1] = _create_page(1)
    pr.base_pages[2] = _create_page(2)
    pr.base_pages[3] = _create_page(3)
    pr.base_pages[4] = _create_page(4)
    pr.base_pages[5] = _create_page(5)

    pr.tail_pages.append(_create_page(6))
    pr.tail_pages.append(_create_page(7))

    pr_bytes = encode_pagerange(pr)
    new_pr = decode_pagerange(pr_bytes)
    
    print(compare_page_ranges(pr, new_pr))


if __name__ == '__main__': test()
