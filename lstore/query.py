from lstore.table import Table, Record
from lstore.index import Index
from BTrees.OOBTree import OOBTree
from lstore.config import *
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table):
        self.table = table
        pass
    
    '''
    # Internal Method
    # Get the address of the base data for the given primary key and column index
    # :param primary_key: int     #The primary key of the record
    # :param column_index: int    #The index of the column
    # :return: list               #The address of the base data
    '''
    def get_base_data_address(self, primary_key, column_index) -> list:
        if column_index >= self.table.num_columns:
            raise ValueError("Column index out of range")
        tree = self.table.index.base_page_indices[column_index]
        if not tree.has_key(primary_key):
            return None
        return tree[primary_key]
    
    '''
    # Internal Method
    # Get the address of the tail data for the given rid and column index
    # :param rid: int             #The rid of the record
    # :param column_index: int    #The index of the column
    # :return: list               #The address of the tail data
    '''
    def get_tail_data_address(self, rid, column_index) -> list:
        if column_index >= self.table.num_columns:
            raise ValueError("Column index out of range")
        tree = self.table.index.tail_page_indices[column_index]
        if not tree.has_key(rid):
            return None
        return tree[rid]
    
    '''
    # Internal Method
    # Modify the value at the given address in a page
    # :param address: list     #The address of the value to be modified
    # :param value: int        #The value to be modified
    '''
    def modify_page_value(self, address: list, value):
        is_base_page = address[0]
        column_index = address[1]
        page_range_index = address[2]
        page_index = address[3]
        record_index = address[4]
        page_dict = self.table.base_page_range_dict if is_base_page else self.table.tail_page_range_dict
        page_dict[column_index][page_range_index].get_page(page_index).modify_value(value, record_index)
    
    '''
    # Internal Method
    # Get the value at the given address in a page
    # :param address: list     #The address of the value to be retrieved
    # :return: int             #The value at the given address
    '''
    def get_page_value(self, address: list) -> int:
        is_base_page = address[0]
        column_index = address[1]
        page_range_index = address[2]
        page_index = address[3]
        record_index = address[4]
        page_dict = self.table.base_page_range_dict if is_base_page else self.table.tail_page_range_dict
        return page_dict[column_index][page_range_index].get_page(page_index).get_value(record_index)
    

    def get_primary_key_address(self, search_key, search_key_index, is_base = True):
        if is_base:
            pagerange_list = self.table.base_page_range_dict[search_key_index]
        else:
            pagerange_list = self.table.tail_page_range_dict[search_key_index]
        address_list = []
        for pagerange in pagerange_list:
            pagerange.get_primary_key_address(search_key, address_list)
        return address_list
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        null_columns = [None] * (self.table.num_columns - 4)
        key_index = self.table.key
        if self.table.index.base_page_indices[key_index].has_key(primary_key):
            self.update(primary_key, *null_columns)
            return True
        else:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns) -> bool:
        key_index = self.table.key
        key = columns[key_index]
        
        if self.table.index.base_page_indices[key_index].has_key(key):
            return False
        
        # Setup the metadata
        indirection = 0
        rid = self.table.current_rid
        self.table.current_rid += 1
        time_stamp = int(time())
        schema_encoding = '0' * (self.table.num_columns - 4)
        
        data = list(columns) + [indirection, rid, time_stamp, schema_encoding]
        record = Record(rid, key, data)
        self.table.write_base_record(record)
        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        if (not self.table.index.base_page_indices[search_key_index].has_key(search_key_index)):
            return False
        
        i = 0
        column_index_list = []
        for val in projected_columns_index:
            if val == 1:
                column_index_list.append(i)
            i += 1

        address_list = self.get_primary_key_address(search_key, search_key_index, True)
        primary_key_list = []
        
        for address in address_list:
            value = self.get_page_value(address)
            primary_key_list.append(value)
        
        result_record_list = []
        
        for primary_key in primary_key_list:
            data_package = []
            latest_update_record_rid = self.get_page_value(self.get_base_data_address(primary_key, self.table.indirection_index))
            for column_index in column_index_list:
                if latest_update_record_rid == 0:
                    data_package.append(self.get_page_value(self.get_base_data_address(primary_key, column_index)))
                if self.table.index.tail_page_indices[column_index].has_key(latest_update_record_rid):
                    data_package.append(self.get_page_value(self.get_tail_data_address(latest_update_record_rid, column_index)))
            record = Record(self.get_page_value(self.get_base_data_address(primary_key, self.table.rid_index)), primary_key, data_package)
            result_record_list.append(record)
        
        return result_record_list
        


                

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns) -> bool:
        key_index = self.table.key
        key = columns[key_index] # Get key value in position index 0
        indirection_index = self.table.indirection_index
        rid_index = self.table.rid_index
        se_index = self.table.schema_encoding_index
        
        if not self.table.index.base_page_indices[key_index].has_key(primary_key):
            return False
        
        if self.get_page_value(self.get_base_data_address(primary_key, indirection_index)) < 10000:
            return False
        
        base_indirection = self.get_page_value(self.get_base_data_address(primary_key, indirection_index))
        
        # Compatible with Delete Function
        delete_detect = True
        
        for i in range(self.table.num_columns - 4):
            if columns[i] != None:
                delete_detect = False
        
        if delete_detect:
            rid = -1
            self.modify_page_value(self.get_base_data_address(primary_key, indirection_index), rid)
            return True
        
        rid = self.table.current_rid
        self.table.current_rid += 1
        schema_encoding_init = ['0'] * (self.table.num_columns - 4) # Assume this is our first update
        
        first_update = False
        
        # If we never updated the record before
        if base_indirection == 0: # Let base record and tail record's indirections point to each other
            self.modify_page_value(self.get_base_data_address(primary_key, indirection_index), rid)
            # Use rid tree to find the base record's rid and assign this rid to the indirection column of new record
            tail_indirection = self.get_page_value(self.get_base_data_address(primary_key, rid_index))
            first_update = True
        # If we updated the record before
        else: # Let tail record's indirection points to the orginal base record's indirection, 
              # then update base record's indirection to tail record's rid
            tail_indirection = base_indirection
            self.modify_page_value(self.get_base_data_address(primary_key, indirection_index), rid)
        
        data_init = [None] * (self.table.num_columns - 4)
        
        for i in range(self.table.num_columns - 4):
            if columns[i] != None:
                data_init[i] = columns[i]
                schema_encoding_init[i] = '1'
        
        for i in range(self.table.num_columns - 4):
            if data_init[i] == None:
                if(first_update):
                    if self.table.index.base_page_indices[i].has_key(primary_key):
                        data_init[i] = self.get_page_value(self.get_base_data_address(primary_key, i))
                else:
                    if self.table.index.tail_page_indices[i].has_key(tail_indirection):
                        data_init[i] = self.get_page_value(self.get_tail_data_address(tail_indirection, i))
                        if self.table.index.base_page_indices[i].has_key(primary_key):
                            if self.get_page_value(self.get_tail_data_address(tail_indirection, i)) != self.get_page_value(self.get_base_data_address(primary_key, i)):
                                schema_encoding_init[i] = '1'
        
        schema_encoding = ''.join(schema_encoding_init)
        
        self.modify_page_value(self.get_base_data_address(primary_key, self.table.schema_encoding_index), schema_encoding)
        
        time_stamp = int(time())
        
        data = list(data_init) + [tail_indirection, rid, time_stamp, schema_encoding]
        record = Record(rid, key, data)
        self.table.write_tail_record(record)
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        terminate_key = 0
        indirection_index = self.table.indirection_index
        for i in range(start_range, end_range + 1):
            if self.table.index.base_page_indices[aggregate_column_index].has_key(i):
                base_indirection = self.get_page_value(self.get_base_data_address(i, indirection_index))
                if base_indirection == 0:
                    result += self.get_page_value(self.get_base_data_address(i, aggregate_column_index))
                    continue
                if self.table.index.tail_page_indices[aggregate_column_index].has_key(base_indirection):
                    result += self.get_page_value(self.get_tail_data_address(base_indirection, aggregate_column_index))
            else:
                terminate_key = i
                break
        if terminate_key == start_range:
            return False
        else:
            return result

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
