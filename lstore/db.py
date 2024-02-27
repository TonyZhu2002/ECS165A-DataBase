from lstore.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self.tables = {}
        self.db_path = ""
        self.buffer_pool = []
        pass

    # Not required for milestone1
    def open(self, path):
        """
        # Open or Reopen the database and load data from disk to buffer-pool
        :param path: the path to physical file
        """
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True) # Create directory if it doesn't exist
        # Try to load existing database file into buffer pool if it exists
        db_file_path = os.path.join(path, "database.pkl")
        if os.path.isfile(db_file_path):
            with open(db_file_path, 'rb') as db_file:
                self.buffer_pool = pickle.load(db_file)
        else:
            self.buffer_pool = []  # Initialize buffer pool if database file does not exist
        pass

    def close(self):
        """
        # Close the database:
        # 1. write dirty pages from buffer-pool to local disk
        # 2. Empty buffer-pool
        """
        # Write buffer pool to disk
        db_file_path = os.path.join(self.db_path, "database.pkl")
        with open(db_file_path, 'wb') as db_file:
            pickle.dump(self.buffer_pool, db_file)
        # Clear the buffer pool
        self.buffer_pool.clear()
        pass

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
        return table

    def drop_table(self, name):
        """
        # Deletes the specified table
        """
        pass

    def get_table(self, name):
        """
        # Returns table with the passed name
        """
        pass
