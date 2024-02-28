from lstore.index import Index
from lstore.page import Page, PageRange
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
from time import time
import copy
import json


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:

    def __init__(self, name, num_columns, key):
        """
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
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
        self.page_index = 0
        self.page_count_dict = {}
        self.index = Index(self)
        self.is_first_read = True
        # Initialize the current rid
        # rid increases by 1 for each record
        self.current_rid = 10000
        self.bufferpool = BufferPool()

        # Initialize the page range list

        for i in range(self.num_all_columns):
            self.__create_page(i, True)
            self.__create_page(i, False)

        # Create index for the every column 
        for i in range(self.num_all_columns):
            if (i != self.key):
                self.index.create_index(i)
    
    def serialize_table(self):
        table_dict = {
            'name': self.name,
            'key': self.key,
            'num_columns': self.num_columns,
            'num_all_columns': self.num_all_columns,
            'indirection_index': self.indirection_index,
            'rid_index': self.rid_index,
            'time_stamp_index': self.time_stamp_index,
            'schema_encoding_index': self.schema_encoding_index,
            'base_record_index': self.base_record_index,
            'tail_record_index': self.tail_record_index,
            'page_range_index': self.page_range_index,
            'page_index': self.page_index,
            'page_count_dict': self.page_count_dict,
            'current_rid': self.current_rid,
        }
        return json.dumps(table_dict)
    
    def deserialize_table(self, json_str):
        table_dict = json.loads(json_str)
        self.name = table_dict['name']
        self.key = table_dict['key']
        self.num_columns = table_dict['num_columns']
        self.num_all_columns = table_dict['num_all_columns']
        self.indirection_index = table_dict['indirection_index']
        self.rid_index = table_dict['rid_index']
        self.time_stamp_index = table_dict['time_stamp_index']
        self.schema_encoding_index = table_dict['schema_encoding_index']
        self.base_record_index = table_dict['base_record_index']
        self.tail_record_index = table_dict['tail_record_index']
        self.page_range_index = table_dict['page_range_index']
        self.page_index = table_dict['page_index']
        self.page_count_dict = table_dict['page_count_dict']
        self.current_rid = table_dict['current_rid']



    def write_base_record(self, record: Record):
        '''
        # Write a column to the base page
        # :param record: Record     #The record to be written to the base page
        # key: data 2rd column: 88 value:
        '''
        columns = record.columns
        curr_page = None
        for i in range(len(columns)):
            curr_page = self.bufferpool.base_page_range_dict[i][-1].get_latest_page()

            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, True)
                curr_page = self.bufferpool.base_page_range_dict[i][-1].get_latest_page()

            curr_page.write(columns[i])

            curr_page.pin += 1
            # If the column is not indexed, create an index for the column
            base_tree = self.index.base_page_indices[i]
            tail_tree = self.index.tail_page_indices[i]
            if not base_tree.has_key(columns[i]):
                base_tree[columns[i]] = []
                tail_tree[columns[i]] = []

            # Add the record num to the index
            base_tree[columns[i]].append(self.base_record_index)
        self.base_record_index += 1
        curr_page.pin -= len(columns)

    '''
    # Write a column to the tail page (Further Test Needed)
    # :param record: Record     #The record to be written to the tail page
    # :param primary_key        #The primary key of the base record
    '''

    def write_tail_record(self, record: Record):
        columns = record.columns
        curr_page = None
        for i in range(len(columns)):
            curr_page = self.bufferpool.tail_page_range_dict[i][-1].get_latest_page()

            # If the page is full, create a new page
            if (not curr_page.has_capacity()):
                self.__create_page(i, False)
                curr_page = self.bufferpool.tail_page_range_dict[i][-1].get_latest_page()
            curr_page.write(columns[i])

            curr_page.pin += 1

            # If the column is not indexed, create an index for the column
            tail_tree = self.index.tail_page_indices[i]

            # Add the record num to the index
            if (columns[i] == None):
                continue
            if not tail_tree.has_key(columns[i]):
                tail_tree[columns[i]] = []
            tail_tree[columns[i]].append(self.tail_record_index)

        self.tail_record_index += 1
        curr_page.pin -= len(columns)

    def serialize_table(self):
        """
        Serialize a Table object to a dictionary.
        """
        table_data = {
            'name': self.name,
            'key': self.key,
            'num_columns': self.num_columns,
            'num_all_columns': self.num_all_columns,
            'indirection_index': self.indirection_index,
            'rid_index': self.rid_index,
            'time_stamp_index': self.time_stamp_index,
            'schema_encoding_index': self.schema_encoding_index,
            'base_record_index': self.base_record_index,
            'tail_record_index': self.tail_record_index,
            'page_range_index': self.page_range_index,
            'page_index': self.page_index,
            'page_count_dict': self.page_count_dict,
            'current_rid': self.current_rid
        }
        return json.dumps(table_data)

    def deserialize_table(self, serialized_str):
        """
        Deserialize a string back to a Table object.
        """
        table_data = json.loads(serialized_str)
        # table = Table(table_data['name'], table_data['num_columns'], table_data['key'])
        self.name = table_data['name']
        self.num_columns = table_data['num_columns']
        self.key = table_data['key']
        self.num_all_columns = table_data['num_all_columns']
        self.indirection_index = table_data['indirection_index']
        self.rid_index = table_data['rid_index']
        self.time_stamp_index = table_data['time_stamp_index']
        self.schema_encoding_index = table_data['schema_encoding_index']
        self.base_record_index = table_data['base_record_index']
        self.tail_record_index = table_data['tail_record_index']
        self.page_range_index = table_data['page_range_index']
        self.page_index = table_data['page_index']
        self.page_count_dict = table_data['page_count_dict']
        self.current_rid = table_data['current_rid']

    def __create_page(self, column_index, is_base_page=True):
        """
        # Create new pages
        # :param page_count: int     #The number of pages to be created
        # :param is_base_page: bool  #True if we want to create base page, False if we want to create tail page
        """
        if (is_base_page):
            page_range_dict = self.bufferpool.base_page_range_dict
            i = 0
        else:
            page_range_dict = self.bufferpool.tail_page_range_dict
            i = 1
        if (page_range_dict.get(column_index) == None):
            page_range_dict[column_index] = [PageRange(MAX_PAGE_RANGE, self.page_range_index)]
            self.page_range_index += 1
        if (self.page_count_dict.get(column_index) == None):
            self.page_count_dict[column_index] = [0, 0]
        self.page_count_dict[column_index][i] += 1
        if (not page_range_dict[column_index][-1].has_capacity()):
            page_range_dict[column_index].append(PageRange(MAX_PAGE_RANGE, self.page_range_index))
            self.page_range_index += 1
        page_range_dict[column_index][-1].add_page(
            Page(self.schema_encoding_index, self.num_all_columns, column_index, self.page_index))
        self.page_index += 1

    def __merge(self):
        print("merge is happening")
        pass
