from lstore.config import *
import struct

class Page:

    def __init__(self, page_range_index, page_index):
        self.num_records = 0
        self.page_range_index = page_range_index
        self.page_index = page_index
        self.data = bytearray(MAX_PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    '''
    # Appends the record to the page
    # :param value: int     #The value to be added to the page
    '''
    def write(self, value) -> list:
        if self.has_capacity:
            begin = self.num_records * 4
            self.data[begin : begin + 4] = struct.pack("i", int(value))   
            self.num_records += 1
            return [self.page_range_index, self.page_index, self.num_records - 1]
        else: 
            raise MemoryError("This Page is full")     

