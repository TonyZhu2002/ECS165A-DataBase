from lstore.table import Table, Record
from lstore.config import *
from copy import deepcopy
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

    def modify_value(self, record_num: int, column_index: int, new_value: int, is_base_page=True):
        """
         # Internal Method
         # Modify the value at the given record number and column index
         # :param record_num: int       #The record number
         # :param column_index: int     #The index of the column
         # :param new_value: int        #The new value
        """

        if (is_base_page):
            page_range_dict = self.table.bufferpool.base_page_range_dict
            indices = self.table.index.base_page_indices
        else:
            page_range_dict = self.table.bufferpool.tail_page_range_dict
            indices = self.table.index.tail_page_indices

        page_range_list = page_range_dict[column_index]
        page_index = record_num // MAX_RECORD_PER_PAGE
        record_index = record_num % MAX_RECORD_PER_PAGE
        page_range_index = page_index // MAX_PAGE_RANGE
        page_index_in_range = page_index % MAX_PAGE_RANGE
        page = page_range_list[page_range_index].get_page(page_index_in_range)
        page.pin += 1

        old_value = page.modify_value(new_value, record_index)
        indices[column_index][old_value].remove(record_num)

        if (new_value not in indices[column_index]):
            indices[column_index][new_value] = []
        indices[column_index][new_value].append(record_num)
        page.pin -= 1

    def get_value(self, record_num: int, column_index: int, is_base_page=True) -> int:
        """
        # Internal Method
        # Get the value at the given record number and column index
        # :param record_num: int       #The record number
        # :param column_index: int     #The index of the column
        # :return: int                 #The value at the given record number and column index
        """
        if (is_base_page):
            page_range_dict = self.table.bufferpool.base_page_range_dict
        else:
            page_range_dict = self.table.bufferpool.tail_page_range_dict

        page_range_list = page_range_dict[column_index]
        page_index = record_num // MAX_RECORD_PER_PAGE
        record_index = record_num % MAX_RECORD_PER_PAGE
        page_range_index = page_index // MAX_PAGE_RANGE
        page_index_in_range = page_index % MAX_PAGE_RANGE
        page = page_range_list[page_range_index].get_page(page_index_in_range)

        return page.get_value(record_index)

    def get_record_list(self, record_num: int, start: int, end: int, is_base_page=True) -> list:
        result = []
        for i in range(start, end):
            result.append(self.get_value(record_num, i, is_base_page))
        return result

    # def traverse_table(self) -> list:
    #     """
    #     # Traverse the table and return all latest records
    #     # :return: list               #The list of all latest records
    #     """
    #     num_base_record = 0
    #     current_rid = self.table.current_rid
    #     record_list = []
    #
    #     if (current_rid == 10000):
    #         return [[]]
    #     # print(current_rid)
    #     for i in range(10000, current_rid):
    #         current_record_list = []
    #         tail_page_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]
    #         if (tail_page_rid_tree.has_key(i)):
    #             continue  # Not a base record
    #         num_base_record += 1
    #         address_list = []
    #         rid_dict = self.table.bufferpool.base_page_range_dict[self.table.rid_index]
    #
    #         for pagerange in rid_dict:
    #             pagerange.get_primary_key_address(i, address_list)
    #         if (len(address_list) == 0):
    #             continue
    #         primary_key = self.get_page_value(address_list[0])
    #
    #         target_record_list = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
    #         if (len(target_record_list) == 0):
    #             continue
    #         for column in target_record_list[0].columns:
    #             current_record_list.append(column)
    #         record_list.append(current_record_list)
    #     return record_list

    def delete(self, primary_key):
        """
        # internal Method
        # Read a record with specified RID
        # Returns True upon successful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """
        primary_key_base_tree = self.table.index.base_page_indices[self.table.key]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]

        null_columns = [None] * (self.table.num_columns)
        key_index = self.table.key
        schema_encoding = '2' * (self.table.num_columns)
        if self.table.index.base_page_indices[key_index].has_key(primary_key):
            base_num_record = primary_key_base_tree[primary_key][0]
            self.modify_value(base_num_record, self.table.schema_encoding_index, schema_encoding)
            self.update(primary_key, *null_columns)
            # Update the latest update record's schema encoding
            this_indirection = self.get_value(base_num_record, self.table.indirection_index, True)
            tail_num_record = tail_rid_tree[this_indirection][0]
            self.modify_value(tail_num_record, self.table.schema_encoding_index, schema_encoding, False)
            return True
        else:
            return False

    def insert(self, *columns) -> bool:
        """
         # Insert a record with specified columns
         # Return True upon successful insertion
         # Returns False if insert fails for whatever reason
         """
        key_index = self.table.key
        key = columns[key_index]

        if self.table.index.base_page_indices[key_index].has_key(key):
            record_num = self.table.index.base_page_indices[key_index][key][0]
            if (self.get_value(record_num, self.table.schema_encoding_index) != '2' * (self.table.num_columns)):
                return False

        # Setup the metadata
        indirection = 0
        rid = self.table.current_rid
        self.table.current_rid += 1
        time_stamp = int(time())
        schema_encoding = '0' * self.table.num_columns

        data = list(columns) + [indirection, rid, time_stamp, schema_encoding]
        record = Record(rid, key, data)
        self.table.write_base_record(record)
        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """

        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
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

        base_target_tree = self.table.index.base_page_indices[search_key_index]
        base_target_list = base_target_tree[search_key]
        tail_target_tree = self.table.index.tail_page_indices[search_key_index]
        tail_target_list = tail_target_tree[search_key]
        base_se_tree = self.table.index.base_page_indices[self.table.schema_encoding_index]
        base_indirection_tree = self.table.index.base_page_indices[self.table.indirection_index]
        base_rid_tree = self.table.index.base_page_indices[self.table.rid_index]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]
        rid_list = []
        result = []

        for se, num_list in base_se_tree.items():
            if (se == '0' * self.table.num_columns):
                for candidate in num_list:
                    expected_value = self.get_value(candidate, search_key_index, True)
                    if (expected_value != search_key):
                        continue
                    primary_key = self.get_value(candidate, self.table.key, True)
                    rid = self.get_value(candidate, self.table.rid_index, True)
                    version_count = self.get_num_version(primary_key)
                    data = []
                    for i in range(self.table.num_columns):
                        if projected_columns_index[i] == 1:
                            data.append(self.get_value(candidate, i, True))
                    result.append(Record(rid, primary_key, deepcopy(data)))
            elif (se == '2' * self.table.num_columns):
                continue
            else:
                for candidate in num_list:
                    primary_key = self.get_value(candidate, self.table.key, True)
                    old_value = self.get_value(candidate, search_key_index, True)
                    indirection = self.get_value(candidate, self.table.indirection_index, True)
                    #print("indirection", indirection, "type", type(indirection))
                    tail_record_num = tail_rid_tree[indirection][0]
                    latest_value = self.get_value(tail_record_num, search_key_index, False)
                    data = []

                    if (latest_value == None and old_value == search_key or latest_value == search_key):
                        version_count = self.get_num_version(primary_key)
                        relative_version = -relative_version
                        if (relative_version > version_count - 2):
                            relative_version = version_count - 2

                        for i in range(relative_version):
                            indirection = self.get_value(tail_record_num, self.table.indirection_index, False)
                            tail_record_num = tail_rid_tree[indirection][0]

                        rid = self.get_value(tail_record_num, self.table.rid_index, False)
                        se = self.get_value(tail_record_num, self.table.schema_encoding_index, False)

                        for i in range(self.table.num_columns):
                            if projected_columns_index[i] == 1 and se[i] == '1':
                                data.append(self.get_value(tail_record_num, i, False))
                            elif projected_columns_index[i] == 1 and se[i] == '0':
                                data.append(self.get_value(candidate, i, True))
                        result.append(Record(rid, primary_key, deepcopy(data)))

        return result

    def update(self, primary_key, *columns) -> bool:
        """
        # Update a record with specified key and columns
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        primary_key_base_tree = self.table.index.base_page_indices[self.table.key]
        primary_key_tail_tree = self.table.index.tail_page_indices[self.table.key]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]

        if (not primary_key_base_tree.has_key(primary_key)):
            return False

        base_num_record = primary_key_base_tree[primary_key][0]
        base_se = self.get_value(base_num_record, self.table.schema_encoding_index, True)
        is_first_update = True

        if base_se == '0' * (self.table.num_columns):
            is_first_update = True
        else:
            is_first_update = False

        # primary key duplication check
        if columns[self.table.key] != None:
            for base_primary_key in primary_key_base_tree:
                base_indirection_loop = self.get_value(primary_key_base_tree[base_primary_key][0],
                                                       self.table.indirection_index, True)
                if base_indirection_loop != 0:
                    tail_latest_primary_key = self.get_value(tail_rid_tree[base_indirection_loop][0], self.table.key,
                                                             False)
                    if tail_latest_primary_key == columns[self.table.key] and base_primary_key != primary_key:
                        return False
                else:
                    if columns[self.table.key] == base_primary_key and base_primary_key != primary_key:
                        return False

        if (is_first_update):
            # Write the snapshot of unmodified record to the tail page
            rid = self.table.current_rid
            self.table.current_rid += 1
            indirection = self.get_value(base_num_record, self.table.rid_index, True)
            time_stamp = int(time())
            schema_encoding = '0' * (self.table.num_columns)
            old_columns = []

            for i in range(self.table.num_columns):
                old_columns.append(self.get_value(base_num_record, i, True))

            data = list(old_columns) + [indirection, rid, time_stamp, schema_encoding]
            self.table.write_tail_record(Record(rid, primary_key, data))

            # Update the base record's indirection
            self.modify_value(base_num_record, self.table.indirection_index, rid, True)

        base_indirection = self.get_value(base_num_record, self.table.indirection_index, True)
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]
        latest_tail_record_num = tail_rid_tree[base_indirection][0]
        latest_schema_encoding = self.get_value(latest_tail_record_num, self.table.schema_encoding_index, False)
        latest_columns = []

        for i in range(self.table.num_columns):
            latest_columns.append(self.get_value(latest_tail_record_num, i, False))

        this_columns = list(columns)
        non_cumulative_schema_encoding = list('0' * (self.table.num_columns))
        new_columns = [None] * (self.table.num_columns)

        for i in range(self.table.num_columns):
            if (this_columns[i] != None):
                non_cumulative_schema_encoding[i] = '1'
                new_columns[i] = this_columns[i]

        cumulative_schema_encoding = list('0' * (self.table.num_columns))

        for i in range(self.table.num_columns):
            if (non_cumulative_schema_encoding[i] == '1'):
                cumulative_schema_encoding[i] = '1'
                continue
            if (latest_schema_encoding[i] == '1'):
                cumulative_schema_encoding[i] = '1'
                new_columns[i] = latest_columns[i]

        cumulative_schema_encoding = ''.join(cumulative_schema_encoding)

        rid = self.table.current_rid
        self.table.current_rid += 1
        indirection = self.get_value(latest_tail_record_num, self.table.rid_index, False)
        time_stamp = int(time())
        schema_encoding = cumulative_schema_encoding
        data = new_columns + [indirection, rid, time_stamp, schema_encoding]
        record = Record(rid, primary_key, data)
        self.table.write_tail_record(record)

        # Update the base record's indirection
        self.modify_value(base_num_record, self.table.indirection_index, rid, True)
        self.modify_value(base_num_record, self.table.schema_encoding_index, cumulative_schema_encoding, True)

        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """
         :param start_range: int         # Start of the key range to aggregate
         :param end_range: int           # End of the key range to aggregate
         :param aggregate_column_index: int  # Index of desired column to aggregate
         # this function is only called on the primary key.
         # Returns the summation of the given range upon success
         # Returns False if no record exists in the given range
         # This function doesn't consider the situation that if the first record is none
         """
        result = 0
        indirection_index = self.table.indirection_index
        record_existence = False

        primary_key_base_tree = self.table.index.base_page_indices[self.table.key]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]

        for i in range(start_range, end_range + 1):
            if self.table.index.base_page_indices[self.table.key].has_key(i):
                base_num_record = primary_key_base_tree[i][0]

                base_se = self.get_value(base_num_record, self.table.schema_encoding_index, True)
                if base_se[0] == '2':
                    continue

                base_indirection = self.get_value(base_num_record, indirection_index, True)
                if base_indirection == 0:
                    result += self.get_value(base_num_record, aggregate_column_index, True)
                    record_existence = True
                    continue
                if self.table.index.tail_page_indices[self.table.rid_index].has_key(base_indirection):
                    tail_record_num = tail_rid_tree[base_indirection][0]
                    if self.get_value(tail_record_num, aggregate_column_index, False) is not None:
                        result += self.get_value(tail_record_num, aggregate_column_index, False)
                    else:
                        result += self.get_value(base_num_record, aggregate_column_index, True)
                    record_existence = True
        if record_existence:
            return result
        else:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
           :param start_range: int         # Start of the key range to aggregate
           :param end_range: int           # End of the key range to aggregate
           :param aggregate_column_index: int  # Index of desired column to aggregate
           :param relative_version: the relative version of the record you need to retrieve.
           # this function is only called on the primary key.
           # Returns the summation of the given range upon success
           # Returns False if no record exists in the given range
           """
        result = 0
        record_existence = False

        primary_key_base_tree = self.table.index.base_page_indices[self.table.key]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]

        for primary_key in range(start_range, end_range + 1):
            if self.table.index.base_page_indices[self.table.key].has_key(primary_key):
                base_num_record = primary_key_base_tree[primary_key][0]

                base_se = self.get_value(base_num_record, self.table.schema_encoding_index, True)
                if base_se[0] == '2':
                    continue

                version_count = self.get_num_version(primary_key)

                if relative_version <= (-version_count + 1):
                    result += self.get_value(base_num_record, aggregate_column_index, True)
                    record_existence = True
                    continue

                this_indirection = self.get_value(base_num_record, self.table.indirection_index, True)
                tail_record_num = tail_rid_tree[this_indirection][0]

                for i in range(0, relative_version, -1):
                    this_indirection = self.get_value(tail_record_num, self.table.indirection_index, False)
                    tail_record_num = tail_rid_tree[this_indirection][0]
                if self.get_value(tail_record_num, aggregate_column_index, False) is not None:
                    result += self.get_value(tail_record_num, aggregate_column_index, False)
                else:
                    result += self.get_value(base_num_record, aggregate_column_index, True)
                record_existence = True

        if record_existence:
            return result
        else:
            return False

    def increment(self, key, column):
        """
        increments one column of the record
        this implementation should work if your select and update queries already work
        :param key: the primary of key of the record to increment
        :param column: the column to increment
        # Returns True is increment is successful
        # Returns False if no record matches key or if target record is locked by 2PL.
        """
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def get_num_version(self, primary_key):
        # Assume the input primary key always exists
        primary_key_base_tree = self.table.index.base_page_indices[self.table.key]
        tail_rid_tree = self.table.index.tail_page_indices[self.table.rid_index]

        base_num_record = primary_key_base_tree[primary_key][0]
        base_indirection = self.get_value(base_num_record, self.table.indirection_index, True)

        if base_indirection == 0:
            return 1
        else:
            version_count = 2
            base_rid = self.get_value(base_num_record, self.table.rid_index, True)

            this_indirection = self.get_value(base_num_record, self.table.indirection_index, True)
            tail_record_num = tail_rid_tree[this_indirection][0]
            next_indirection = self.get_value(tail_record_num, self.table.indirection_index, False)

            while next_indirection != base_rid:
                version_count += 1

                this_indirection = next_indirection

                tail_record_num = tail_rid_tree[next_indirection][0]
                next_indirection = self.get_value(tail_record_num, self.table.indirection_index, False)

            return version_count

        pass
