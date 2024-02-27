from lstore.config import *
import struct

class Page:

    def __init__(self, schema_encoding_index, num_col, column_index, page_index):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)
        self.schema_encoding_index = schema_encoding_index
        self.num_col = num_col
        self.column_index = column_index
        self.page_index = page_index

    '''
    # Check if the page has capacity for more records
    # :return: bool     #True if the page has capacity for more records, False otherwise
    '''
    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    '''
    # Appends the record to the page
    # :param value: int     #The value to be added to the page
    # :return: list         #The location of the record in the page
    # return None if the column is None
    '''
    def write(self, value) -> list:
        # This always happens when we write column for the tail page
        if value == None:
            value = NONE_VALUE
        if self.has_capacity:
            begin = self.num_records * 4
            self.data[begin : begin + 4] = struct.pack("i", int(value))   
            self.num_records += 1
        else: 
            raise MemoryError("This Page is full")
    '''   
    def get_primary_key_address(self, key_value, dest_list):
        for i in range(self.num_records):
            value = self.get_value(i)
            if value == key_value:
                address = [self.is_base_page, self.table_key_index, self.page_range_index, self.page_index, i]
                dest_list.append(address)
    ''' 
    
    '''
    # Modify the value at the given index
    # :param value: int     #The value to be modified
    # :param index: int     #The index of the value to be modified
    '''    
    def modify_value(self, value, index):
        begin = index * 4
        old_value = self.get_value(index)
        self.data[begin : begin + 4] = struct.pack("i", int(value))
        return old_value
        
    '''
    # Get the value at the given index
    # :param index: int     #The index of the value to be retrieved
    # :return: int          #The value at the given index
    '''  
    def get_value(self, index):
        begin = index * 4
        value = struct.unpack("i", self.data[begin : begin + 4])[0]
        if (self.column_index == self.schema_encoding_index):
            value = str(value)
            value = value.rjust(self.num_col - 4, '0')
        if (value == NONE_VALUE):
            value = None
        return value
    
class PageRange:
    def __init__(self, max_capacity, page_range_index):
        # The maximum number of pages that can be stored in this page range
        self.max_capacity = max_capacity
        
        # For example, [Page1, Page2, Page3, ..., PageMaxCapacity]
        self.pages = []
        self.page_range_index = page_range_index
    
    '''
    # Check if the page range has capacity for more pages
    # :return: bool     #True if the page range has capacity for more pages, False otherwise
    '''
    def has_capacity(self):
        return len(self.pages) < self.max_capacity
    
    '''
    # Add a page to the page range
    '''
    def add_page(self, page: Page):
        if self.has_capacity():
            self.pages.append(page)
        else:
            raise MemoryError("This PageRange is full")
    
    '''
    # Get the latest page in the page range
    # :return: Page     #The latest page in the page range
    '''
    def get_latest_page(self) -> Page:
        return self.pages[-1]
    
    '''
    # Get the page at the given index
    # :param index: int     #The index of the page to be retrieved
    # :return: Page         #The page at the given index
    '''
    def get_page(self, index) -> Page:
        return self.pages[index]
    '''
    def get_primary_key_address(self, key_value, dest_list):
        for page in self.pages:
            page.get_primary_key_address(key_value, dest_list)
    '''
