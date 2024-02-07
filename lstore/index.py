from BTrees.OOBTree import OOBTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        if (self.indices[table.key] == None):
            self.indices[table.key] = OOBTree()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if (self.indices[column_number] == None):
            self.indices[column_number] = OOBTree()
        else:
            print(f'Index already exists in column {column_number}!')

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if (self.indices[column_number] != None):
            self.indices[column_number] = None
        else:
            print(f'Index does not exist in column {column_number}!')
