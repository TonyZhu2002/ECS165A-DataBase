from BTrees.OOBTree import OOBTree
import json


"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.base_page_indices = [None] * table.num_all_columns
        self.tail_page_indices = [None] * table.num_all_columns
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

    def recreate_index(self, column_number, tree, page_type):
        page_dict = self.base_page_indices if page_type == "base" else self.tail_page_indices
        page_dict[column_number] = tree
        
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

    def serialize_oobtree(tree):
        # Convert the OOBTree to a list of tuples (key, value) for serialization
        items = list(tree.items())
        return json.dumps(items)

    def deserialize_oobtree(serialized_str):
        # Deserialize the JSON string back to a list of tuples
        items = json.loads(serialized_str)
        # Reconstruct the OOBTree
        tree = OOBTree()
        for key, value in items:
            tree[key] = value
        return tree
