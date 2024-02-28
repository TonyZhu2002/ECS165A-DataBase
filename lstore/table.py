from lstore.index import Index
from lstore.page import Page, PageRange
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
        self.num_columns = num_columns
        self.num_all_columns = num_columns + 4
        self.indirection_index = self.num_all_columns - 4
        self.rid_index = self.num_all_columns - 3
        self.time_stamp_index = self.num_all_columns - 2
        self.schema_encoding_index = self.num_all_columns - 1
        self.base_record_index = 0
        self.tail_record_index = 0
        self.page_range_index = 0
        self.page_count = 0

        # Initialize the page range dictionary
        # The key is the column index
        # The value is a list of page ranges
        self.base_page_range_dict = {}
        self.tail_page_range_dict = {}

        # Initialize the page count dictionary
        # The key is the column index
        # The value is a list of two integers [base_page_count, tail_page_count]
        # base_page_count is the number of base pages for the column
        # tail_page_count is the number of tail pages for the column
        self.page_count_dict = {}

        self.index = Index(self)

        # Initialize the current rid
        # rid increases by 1 for each record
        self.current_rid = 10000

        # Initialize the page range list
        for i in range(self.num_all_columns):
            self.__create_page(i, True)
            self.__create_page(i, False)

        # Create index for the every column 
        for i in range(self.num_all_columns):
            if (i != self.key):
                self.index.create_index(i)

    '''
    # Write a column to the base page
    # :param record: Record     #The record to be written to the base page
    # key: data 2rd column: 88 value: 
    '''

    def write_base_record(self, record: Record):
        columns = record.columns
        for i in range(len(columns)):
            curr_page = self.base_page_range_dict[i][-1].get_latest_page()

            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, True)
                curr_page = self.base_page_range_dict[i][-1].get_latest_page()
            curr_page.write(columns[i])

            # If the column is not indexed, create an index for the column
            base_tree = self.index.base_page_indices[i]
            tail_tree = self.index.tail_page_indices[i]
            if not base_tree.has_key(columns[i]):
                base_tree[columns[i]] = []
                tail_tree[columns[i]] = []

            # Add the record num to the index
            base_tree[columns[i]].append(self.base_record_count)
        self.base_record_count += 1

    '''
    # Write a column to the tail page (Further Test Needed)
    # :param record: Record     #The record to be written to the tail page
    # :param primary_key        #The primary key of the base record
    '''

    def write_tail_record(self, record: Record):
        columns = record.columns
        for i in range(len(columns)):
            curr_page = self.tail_page_range_dict[i][-1].get_latest_page()

            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, False)
                curr_page = self.tail_page_range_dict[i][-1].get_latest_page()
            curr_page.write(columns[i])

            # If the column is not indexed, create an index for the column
            tail_tree = self.index.tail_page_indices[i]

            # Add the record num to the index
            if (columns[i] == None):
                continue
            if not tail_tree.has_key(columns[i]):
                tail_tree[columns[i]] = []
            tail_tree[columns[i]].append(self.tail_record_count)

        self.tail_record_count += 1



    def __create_page(self, column_index, is_base_page=True):
        """
        # Create new pages
        # :param page_count: int     #The number of pages to be created
        # :param is_base_page: bool  #True if we want to create base page, False if we want to create tail page
        """
        if (is_base_page):
            page_range_dict = self.base_page_range_dict
            i = 0
        else:
            page_range_dict = self.tail_page_range_dict
            i = 1
        if (page_range_dict.get(column_index) == None):
            page_range_dict[column_index] = [PageRange(MAX_PAGE_RANGE, self.page_range_index)]
            self.page_range_index += 1
        if (self.page_index_dict.get(column_index) == None):
            self.page_index_dict[column_index] = [0, 0]
        self.page_index_dict[column_index][i] += 1
        if (not page_range_dict[column_index][-1].has_capacity()):
            page_range_dict[column_index].append(PageRange(MAX_PAGE_RANGE, self.page_range_index))
            self.page_range_index += 1
        page_range_dict[column_index][-1].add_page(
            Page(self.schema_encoding_index, self.num_all_columns, column_index, self.page_index))
        self.page_index += 1

    def __merge(self):
        print("merge is happening")
        pass
