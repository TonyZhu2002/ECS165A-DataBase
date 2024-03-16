from lstore.table import Table, Record
from lstore.index import Index
from db import Database
from query import Query
from bufferpool import BufferPool


class Transaction:

    def __init__(self):
        """
        # Creates a transaction object.
        """
        self.queries = []
        self.old_value_keeper = []
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
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            self.old_value_keeper.append(self.rollback_method(query, args))
        return self.commit()

    def rollback_method(self, function, *args):
        query_name = function.__name__
        if query_name == "insert":
            return (Query(self.table).delete, args)
        elif query_name == "delete":
            return (Query(self.table).insert, args)
        elif query_name == "update":
            return (Query(self.table).update, args)

        # Assume it returns a tuple similar to add_query, but contains rollback function with necessary arguments.
    def abort(self):
        # TODO: do roll-back and any other necessary operations
        for query, args in reversed(self.old_value_keeper):
            query(*args)
        self.old_value_keeper.clear()
        return False

    def commit(self):
        # TODO: commit to database
        return True
