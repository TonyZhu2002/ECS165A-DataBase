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

        # Initialize the page list
        self.__create_page(2 * self.num_columns)
        
        # Create index for the every column 
        for i in range(self.num_columns):
            if (i != self.key):
                self.index.create_index(i)
                
    
    '''
    # Write a column to the base page
    # :param record: Record     #The record to be written to the base page
    '''
    def write_base_record(self, record: Record):
        if self.__check_page_full():
            self.__allocate_base_page()
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
        if self.__check_page_full():
            self.__allocate_base_page()
        column = record.columns
        for i in range(len(column)):
            address = self.__find_page(i, False).write(column[i])
            self.index.indices[i][record.key] = address
            
        # Update Indirection Column of Base Record
        self.index.indices[4][primary_key] = column[4]
        
    
    '''
    # find the newest page based on the column index
    # :param col_index: int     #The index of the column
    # :param is_base_page: bool #True if we want to find the base page, False if we want to find the tail page
    '''    
    def __find_page(self, col_index, is_base_page = True) -> Page:
        if (self.allocate_count == 0):
            range_index = (col_index * 2) // MAX_PAGE_RANGE
            page_index = (col_index * 2) % MAX_PAGE_RANGE
            return self.page_list[range_index][page_index] if is_base_page else self.page_list[range_index][page_index + 1]
        elif (is_base_page) == False:
            return self.page_list[range_index][page_index + 1]
        else:
            range_index = (self.num_columns * (self.allocate_count + 1) + col_index) // MAX_PAGE_RANGE
            page_index = (self.num_columns * (self.allocate_count + 1) + col_index) % MAX_PAGE_RANGE
            #print(f'range_index: {range_index}, page_index: {page_index}')
            return self.page_list[range_index][page_index]
        
    '''
    # Create new pages
    # :param page_count: int     #The number of pages to be created
    '''
        
    def __create_page(self, page_count):
        for i in range(page_count):
            if len(self.page_list[-1]) == MAX_PAGE_RANGE:
                self.page_list.append([])
            self.page_list[-1].append(Page(len(self.page_list) - 1, len(self.page_list[-1])))
    
    def __check_page_full(self):
        if (self.allocate_count == 0):
            return not self.page_list[-1][-2].has_capacity()
        else:
            return not self.page_list[-1][-1].has_capacity()
    '''
    Allocate new pages based on the number of columns
    '''
    def __allocate_base_page(self):
        self.allocate_count += 1
        self.__create_page(self.num_columns)
        print('page_list_length:', len(self.page_list))
        print('last_page_length:', len(self.page_list[-1]))
        print('num_record_in_last_page:', self.page_list[1][10].num_records)
                               

    def __merge(self):
        print("merge is happening")
        pass
 
