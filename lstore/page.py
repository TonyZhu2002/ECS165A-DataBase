from lstore.config import *
import struct

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    '''
    # Appends the record to the page
    # :param value: int   #The value to be added to the page
    '''
    def write(self, value):
        if self.has_capacity:
            begin = self.num_records * 4   
            self.data[begin : begin + 4] = struct.pack("i", value)   
            self.num_records += 1
        else: 
            raise MemoryError("Page is full")     

