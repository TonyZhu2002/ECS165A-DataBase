from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from time import time
import copy

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
        self.num_columns = num_columns + 4
        self.page_list = [[]]
        self.index = Index(self)
        self.current_rid = 10000
        self.allocate_count = 0
        # Every column should have a base page and a tail page
        
        '''
        # We initialize the page list with the number of pages we need
        # The page list will look like this: [[base_page1, tail_page1, base_page2, tail_page2, ...], [base_page1, tail_page1, base_page2, tail_page2, ...], ...]
        '''
        self.__create_page(2 * self.num_columns)
        print('Done initializing page_list')
        
        # Create index for the every column 
        for i in range(self.num_columns):
            if (i != self.key):
                self.index.create_index(i)
                
    def __create_page(self, page_count):
        for i in range(page_count):
            if len(self.page_list[-1]) == MAX_PAGE_RANGE:
                self.page_list.append([])
            else:
                self.page_list[-1].append(Page(len(self.page_list) - 1, len(self.page_list[-1])))
    
    '''
    # Write a column to the base page
    # :param record: Record     #The record to be written to the base page
    '''
    def write_base_record(self, record: Record):
        column = record.columns
        for i in range(len(column)):
            address = self.__find_page(i, True).write(column[i])
            self.index.indices[i][record.key] = address
    
    '''
    # Write a column to the tail page (Further Test Needed)
    # :param record: Record     #The record to be written to the tail page
    # :param primary_key        #The primary key of the base record
    '''
    def write_tail_record(self, primary_key, record: Record):
        column = record.columns
        for i in range(len(column)):
            address = self.__find_page(i, False).write(column[i])
            self.index.indices[i][record.key] = address
        # Update Indirection Column of Base Record
        self.index.indices[4][primary_key] = column[4]
    
    '''
    # find a page based on the column index
    # :param col_index: int     #The index of the column
    # :param is_base_page: bool #True if we want to find the base page, False if we want to find the tail page
    '''    
    def __find_page(self, col_index, is_base_page = True) -> Page:
        if (self.allocate_count == 0):
            range_index = (col_index * 2) // MAX_PAGE_RANGE
            page_index = (col_index * 2) % MAX_PAGE_RANGE
            return self.page_list[range_index][page_index] if is_base_page else self.page_list[range_index][page_index + 1]
        elif (is_base_page) == False:
            raise Exception("Tail page not found, check your arguments")
        else:
            raise NotImplementedError("Page allocation not implemented")
            
                 
                       

    def __merge(self):
        print("merge is happening")
        pass
 
