from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from time import time
import copy

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_list = []
        self.index = Index(self)
        
        # Map the meta pages
        INDIRECTION_COLUMN += num_columns
        RID_COLUMN += num_columns
        TIMESTAMP_COLUMN += num_columns
        SCHEMA_ENCODING_COLUMN += num_columns
        
        # Every column should have a base page and a tail page
        num_page = 2 * (num_columns+4)
        
        '''
        # We initialize the page list with the number of pages we need
        # The page list will look like this: [[base_page1, tail_page1, base_page2, tail_page2, ...], [base_page1, tail_page1, base_page2, tail_page2, ...], ...]
        '''
        page_range_list = []
        count = 0
        for i in range(num_page):
            if (count < MAX_PAGE_RANGE):
                page_range_list.append(Page())
                count += 1
            else:
                self.page_list.append(copy.deepcopy(page_range_list))
                page_range_list.clear()
                count = 0
        
        if (count > 0):
            self.page_list.append(copy.deepcopy(page_range_list))
            page_range_list.clear()       
    
    '''
    # find a page based on the column index
    # :param col_index: int     #The index of the column
    # :param is_base_page: bool #True if we want to find the base page, False if we want to find the tail page
    '''    
    def __find_page(self, col_index, is_base_page = True) -> Page:
        range_index = (col_index * 2) // MAX_PAGE_RANGE
        page_index = (col_index * 2) % MAX_PAGE_RANGE
        return self.page_list[range_index][page_index] if is_base_page else self.page_list[range_index][page_index + 1]       
                       

    def __merge(self):
        print("merge is happening")
        pass
 
