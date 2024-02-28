from lstore.table import Table
import os
from BTrees.OOBTree import OOBTree
import base64
import json


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


class Database():

    def __init__(self):
        self.tables = {}
        self.db_path = ""
        self.buffer_pool = []
        # pass


    def open(self, path):
        """
        Open or Reopen the database and load data from disk to buffer-pool.
        :param path: the path to physical file
        """
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)  # Create directory if it doesn't exist

        for table_name in os.listdir(path):
            table_path = os.path.join(path, table_name)
            if os.path.isdir(table_path):
                # Initialize or clear existing table structure in memory
                with open(table_path, 'r') as file:
                    serialized_page = file.read()
                table = self.create_table(table_name,5, 0)

                for column_dir in os.listdir(table_path):
                    column_path = os.path.join(table_path, column_dir)
                    if os.path.isdir(column_path):
                        for page_type in ["base", "tail"]:
                            page_type_path = os.path.join(column_path, page_type)
                            if os.path.isdir(page_type_path):
                                tree_file_path = os.path.join(page_type_path, f"{page_type}.txt")
                                with open(tree_file_path, 'r') as file:
                                    serialized_tree = file.read()
                                tree = deserialize_oobtree(serialized_tree)
                                # Assuming you have a method to set the OOBTree for the column
                                table.set_oobtree_for_column(column_dir, page_type, tree)

                                for page_range_dir in os.listdir(page_type_path):
                                    page_range_path = os.path.join(page_type_path, page_range_dir)
                                    if os.path.isdir(page_range_path):
                                        for page_file in os.listdir(page_range_path):
                                            page_file_path = os.path.join(page_range_path, page_file)
                                            with open(page_file_path, 'r') as file:
                                                serialized_page = file.read()
                                            page = serialized_page.deserialize(serialized_page)
                                            # Assuming you have a method to add the page back to the buffer pool
                                            table.add_page_to_bufferpool(column_dir, page_type, page_range_dir, page)

        # After loading, perform any necessary initializations or buffer pool population

    def close(self):
        """
        Close the database:
        1. Write dirty pages from buffer pool to local disk.
        2. Empty buffer pool.
        """
        # Iterate through each table and its associated data
        for table_name, table in self.tables.items():
            table_path = os.path.join(self.db_path, table_name)
            if not os.path.isdir(table_path):
                os.makedirs(table_path)
            
            table_config_path = os.path.join(table_path, f"{table_name}_tab_config.txt")
            with open(table_config_path, "w") as config_file:
                config_file.write(table.serialize_table())

            # Iterate through each column
            for column_index in range(table.num_all_columns):
                column_path = os.path.join(table_path, f"{column_index}")
                if not os.path.isdir(column_path):
                    os.makedirs(column_path)

                # Process both base and tail for each column
                for page_type in ["base", "tail"]:
                    type_path = os.path.join(column_path, page_type)
                    if not os.path.isdir(type_path):
                        os.makedirs(type_path)

                    # Serialize and write the OOBTree index
                    tree = table.index.base_page_indices[column_index] if page_type == "base" else table.index.tail_page_indices[column_index]
                    serialized_tree = serialize_oobtree(tree)
                    tree_file_path = os.path.join(type_path, f"{page_type}_index.txt")
                    with open(tree_file_path, "w") as file:
                        file.write(serialized_tree)

                    # Determine the appropriate page range dictionary
                    page_range_dict = table.bufferpool.base_page_range_dict if page_type == "base" else table.bufferpool.tail_page_range_dict

                    # Serialize and write the pages
                    for page_range_index, page_range in page_range_dict.items():
                        page_range_path = os.path.join(type_path, f"{page_range_index}")
                        if not os.path.isdir(page_range_path):
                            os.makedirs(page_range_path)
                        for individual_page_range in page_range:
                            for page_index, page in enumerate(individual_page_range.pages):
                                serialized_page = page.serialize()
                                page_file_path = os.path.join(page_range_path, f"page{page_index}.txt")
                                with open(page_file_path, "w") as file:
                                    file.write(serialized_page)

        # Clear the buffer pool
        self.buffer_pool.clear()

    def create_table(self, name, num_columns, key_index):
        """
         # Creates a new table
         :param name: string         #Table name
         :param num_columns: int     #Number of Columns: all columns are integer
         :param key: int             #Index of table key in columns
         """
        if self.tables.get(name) != None:
            raise ValueError(f'Table {name} already exists!')
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        self.buffer_pool.append(table.bufferpool)
        return table

    def drop_table(self, name):
        """
        # Deletes the specified table
        """
        if self.tables.get(name) == None:
            raise ValueError(f'Table {name} already deleted!')
        else:
            del self.tables[name]
        pass

    def get_table(self, name):
        """
        # Returns table with the passed name
        """
        for table in self.tables:
            if table.name == name:
                return table
        pass
