from lstore.table import Table, Record
from lstore.index import Index
import threading


class TransactionWorker():
    """
    # Creates a transaction worker object.
    """

    def __init__(self, transactions=[]):
        # self.stats = []
        # self.transactions = transactions
        # self.result = 0
        self.transactions = transactions
        self.threads = []

    def add_transaction(self, t):
        """
        Appends t to transactions
        """
        # self.transactions.append(t)
        self.transactions.append(t)

    def run(self):
        # Create and start a thread for each transaction
        for transaction in self.transactions:
            thread = threading.Thread(target=transaction.run)
            self.threads.append(thread)
            thread.start()

    def join(self):
        """
        Waits for the worker to finish
        """
        for thread in self.threads:
            thread.join()
        pass

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
