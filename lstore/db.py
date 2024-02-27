from lstore.table import Table


class Database():

    def __init__(self):
        self.tables = {}
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
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
