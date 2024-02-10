from BTrees.OOBTree import OOBTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.base_page_indices = [None] * table.num_columns
        self.tail_page_indices = [None] * table.num_columns
        if (self.base_page_indices[table.key] == None):
            self.base_page_indices[table.key] = OOBTree()
        if (self.tail_page_indices[table.key] == None):
            self.tail_page_indices[table.key] = OOBTree()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        index = self.indices[column]
        if index is not None:
            return index.get(value, [])
        else:
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        index = self.indices[column]
        if index is not None:
            # Fetch a range of keys using B-Tree's efficient range search capability
            return [rid for key, rid in index.items(begin, end)]
        else:
            return []

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if (self.base_page_indices[column_number] == None):
            self.base_page_indices[column_number] = OOBTree()
        if (self.tail_page_indices[column_number] == None):
            self.tail_page_indices[column_number] = OOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if (self.base_page_indices[column_number] != None):
            self.base_page_indices[column_number] = None
        if (self.tail_page_indices[column_number] != None):
            self.tail_page_indices[column_number] = None
