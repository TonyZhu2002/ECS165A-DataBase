from lstore.table import Table, Record
from lstore.index import Index
from lstore.db import Database
from lstore.query import Query
from lstore.bufferpool import BufferPool
from copy import deepcopy


class Transaction:

    def __init__(self):
        """
        # Creates a transaction object.
        """
        self.queries = []
        # self.old_value_keeper = []
        self.old_buffer_pool = None
        self.old_index = None
        self.table = None
        pass

    def add_query(self, query, table, *args):
        """
        # Adds the given query to this transaction
        # Example:
        # q = Query(grades_table)
        # t = Transaction()
        # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """
        self.queries.append((query, args))
        self.table = table
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self.old_buffer_pool = deepcopy(self.table.bufferpool)
        self.old_index = deepcopy(self.table.index)
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # self.old_value_keeper.append(self.rollback_method(query, args))
        return self.commit()
        
        
    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # for query, args in reversed(self.old_value_keeper):
        #     query(*args)
        self.table.bufferpool = deepcopy(self.old_buffer_pool)
        self.table.index = deepcopy(self.old_index)
        # self.old_value_keeper.clear()
        return False

    def commit(self):
        # TODO: commit to database
        return True

    # def rollback_method(self, function, *args):
    #     query_name = function.__name__
    #     if query_name == "insert":
    #         return (Query(self.table).delete, args)
    #     elif query_name == "delete":
    #         return (Query(self.table).insert, args)
    #     elif query_name == "update":
    #         primary_key = args[0]
    #         recover_col = []
    #         for column_index in range(self.table.num_columns):
    #             pass
    #         return (Query(self.table).update, [args[0], recover_col])

        # Assume it returns a tuple similar to add_query, but contains rollback function with necessary arguments.