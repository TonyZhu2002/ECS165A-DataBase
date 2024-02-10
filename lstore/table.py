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
        self.base_page_dict = {}
        self.tail_page_dict = {}
        self.page_count_dict = {}
        self.index = Index(self)
        self.current_rid = 10000


        # Initialize the page list
        for i in range(self.num_columns):
            self.__create_page(i, True)
            self.__create_page(i, False)
        
        # Create index for the every column 
        for i in range(self.num_columns):
            if (i != self.key):
                self.index.create_index(i)
                
    '''
    # Write a column to the base page
    # :param record: Record     #The record to be written to the base page
    '''
    def write_base_record(self, record: Record):
        columns = record.columns
        for i in range(len(columns)):
            curr_page = self.__find_page(i, True)
            
            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, True)
                curr_page = self.__find_page(i, True)
            address = curr_page.write(columns[i])
            if (address != None):
                self.index.indices[i][record.key] = address
    
    '''
    # Write a column to the tail page (Further Test Needed)
    # :param record: Record     #The record to be written to the tail page
    # :param primary_key        #The primary key of the base record
    '''
    def write_tail_record(self, record: Record):
        columns = record.columns
        for i in range(len(columns)):
            curr_page = self.__find_page(i, False)
            
            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, False)
                curr_page = self.__find_page(i, False)
            address = curr_page.write(columns[i])
            if (address != None):
                self.index.indices[i][record.key] = address
        
    '''
    # find the newest page based on the column index
    # :param col_index: int     #The index of the column
    # :param is_base_page: bool #True if we want to find the base page, False if we want to find the tail page
    # :return: Page             #The page that we want to find
    '''    
    def __find_page(self, col_index, is_base_page = True) -> Page:
        return self.base_page_dict[col_index][-1][-1] if is_base_page else self.tail_page_dict[col_index][-1][-1]
        
    '''
    # Create new pages
    # :param page_count: int     #The number of pages to be created
    # :param is_base_page: bool  #True if we want to create base page, False if we want to create tail page
    '''     
    def __create_page(self, column_index, is_base_page = True):
        print("Creating base page" if is_base_page else "Creating tail page")
        if (is_base_page):
            page_dict = self.base_page_dict
            i = 0
        else:
            page_dict = self.tail_page_dict
            i = 1
        if (page_dict.get(column_index) == None):
            page_dict[column_index] = [[]]
        if (self.page_count_dict.get(column_index) == None):
            self.page_count_dict[column_index] = [0, 0]
        self.page_count_dict[column_index][i] += 1
        if (len(page_dict[column_index]) >= MAX_PAGE_RANGE):
            page_dict[column_index].append([])
        page_range_index = (self.page_count_dict[column_index][i] - 1) // MAX_PAGE_RANGE
        page_index = (self.page_count_dict[column_index][i] - 1) % MAX_PAGE_RANGE
        page_dict[column_index][-1].append(Page(column_index, page_range_index, page_index, is_base_page))
                               
    def __merge(self):
        print("merge is happening")
        pass
 
