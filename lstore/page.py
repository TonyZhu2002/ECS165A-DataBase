from lstore.config import *
import struct

class Page:

    def __init__(self, column_index, page_range_index, page_index, is_base_page):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)
        self.column_index = column_index
        self.page_range_index = page_range_index
        self.page_index = page_index
        self.is_base_page = is_base_page

    '''
    # Check if the page has capacity for more records
    # :return: bool     #True if the page has capacity for more records, False otherwise
    '''
    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    '''
    # Appends the record to the page
    # :param value: int     #The value to be added to the page
    '''
    def write(self, value) -> list:
        # If we do not have any value for this column of the record, we will return None
        # This always happens when we write column for the tail page
        if value == None:
            return None
        if self.has_capacity:
            begin = self.num_records * 4
            self.data[begin : begin + 4] = struct.pack("i", int(value))   
            self.num_records += 1
            return [self.is_base_page, self.column_index, self.page_range_index, self.page_index, self.num_records - 1]
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
