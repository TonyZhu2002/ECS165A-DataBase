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
    
    '''
    # Modify the value at the given index
    # :param value: int     #The value to be modified
    # :param index: int     #The index of the value to be modified
    '''    
    def modify_value(self, value, index):
        begin = index * 4
        self.data[begin : begin + 4] = struct.pack("i", int(value))
        
    '''
    # Get the value at the given index
    # :param index: int     #The index of the value to be retrieved
    # :return: int          #The value at the given index
    '''  
    def get_value(self, index) -> int:
        begin = index * 4
        return struct.unpack("i", self.data[begin : begin + 4])[0]
