from config import *
import threading

### Utility functions for the database innerworkings ###

# Normalizes string, converts to lowercase, removes non-alpha characters, and converts spaces to hyphens.
def sanitize(table_name):
    import string
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in table_name if c in valid_chars)
    filename = filename.split(" ")
    filename = '_'.join(c for c in filename if c != "")
    
    
    return filename

# Takes bytes of schema encoding and converts it to a string
def parse_schema_enc_from_bytes(enc_bytes):

    ret_enc = ''
    for byte in enc_bytes:
        ret_enc += '' + str(byte)
    return ret_enc


def int_from_bytes(from_bytes):
    return int.from_bytes(from_bytes, BYTE_ORDER)

    
# Int from bytes
def ifb(_bytes):
    return int_from_bytes(_bytes)


def int_to_bytes(data):
    return data.to_bytes(CELL_SIZE_BYTES, BYTE_ORDER)

# Int to bytes
def itb(data):
    return int_to_bytes(data)


### Gets inner index of value ###
# :param outer_index: int       #Overall place within list
# :param container_size: int    #Size of container within list
#   Example with container size = 3
#   | 0 1 2 | | 3 4 5 | | 6 7 8 |
#    Inner_index = 0 for 0, 3, 6
#    Inner_index = 1 for 1, 4, 7
#    Inner_index = 2 for 2, 5, 6
def get_inner_index_from_outer_index(outer_index, container_size):

    if outer_index < container_size:
        return outer_index

    for i in range(container_size):
        base_index = i
        mult = (outer_index - base_index) / (container_size)  # type: float

        if mult.is_integer():
            break

    return base_index

# Gets a latch to lock a variable
def acquire_latch(lock):
    while(True):
        is_acquired = lock.acquire(False)
        if is_acquired:
            return lock

# Self explanatory
def acquire_all(locks):
    acquired = []

    for lock in locks:
        is_acquired = lock.acquire(False)

        if not is_acquired:
            for to_release in acquired:
                to_release.release()

            # print('Couldn\'t acquire all locks')
            return False

        acquired.insert(0, lock)

    return acquired

# Self explanatory
def release_all(locks):
    for lock in locks:
        lock.release()

# Takes list of numbers of schema encoding and converts it to binary
def col_encoding_to_binary(list, truthy=1, falsey=0):
    bin = 0
    for i, bit in enumerate(list):
        if bit == truthy or bit != falsey:
            bin += 2**i

    return bin

def cetb(list, truthy=1, falsey=0):
  '''
  Column encoding to binary number
  
  cetb((OBJ, None, OBJ, None), falsey=None)
  '''
  return col_encoding_to_binary(list, truthy, falsey)


def check_col_encoding(bin_enc, col):
    '''
    [0,1,2,3]
    '''
    mask = 2**col
    return mask & bin_enc == mask

### Converts pid to bytes ###
# :param pid:       #Tuple of inner_idx, page_idx, page_range_idx
# :return bytes:    #Bytes of a pid representing inner_idx, page_idx, page_range_idx
def encode_pid(pid):  # 24 bytes
    out = b''
    for idx in pid:
        out += int_to_bytes(idx)

    return out

### Converts bytes to pid ###
# :param bytes_pid: #Bytes of a pid representing inner_idx, page_idx, page_range_idx
# :return tuple:    #Tuple of inner_idx, page_idx, page_range_idx
def decode_pid(bytes_pid):  # 24 bytes
    inner_idx = int_from_bytes(bytes_pid[0:8])
    page_idx = int_from_bytes(bytes_pid[8:16])
    pr_idx = int_from_bytes(bytes_pid[16:24])

    return (inner_idx, page_idx, pr_idx)

#testing stuff
def test():
    enc = [(1, 0, 0), None, (1, 0, 0), None]
    bin = col_encoding_to_binary(enc, falsey=None)
    by = int_to_bytes(bin)
    read_enc = int_from_bytes(by)
    print([check_col_encoding(read_enc, i) for i in range(4)])

    pid = (123, 456, 789)
    print(decode_pid(encode_pid(pid)))

class Counter:
    def __init__(self, initial_val):
        self.crit = threading.Lock()
        self.val = initial_val

    def get(self):
        with self.crit:
            return self.val

    def increment(self):
        with self.crit:
            self.val += 1
            return self.val

    def decrement(self):
        with self.crit:
            self.val -= 1
            return self.val


if __name__ == '__main__':
    test()
