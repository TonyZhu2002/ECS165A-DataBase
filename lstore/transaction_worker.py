from lstore.table import Table, Record
from lstore.index import Index


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """

    def __init__(self, transactions=[]):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        """
        Appends t to transactions
        """
        self.transactions.append(t)

    def run(self):
        """
        Runs all transaction as a thread
        """
        pass
        # here you need to create a thread and call __run

    def join(self):
        """
        Waits for the worker to finish
        """
        pass

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
