from table import Table, Record
from index import Index
import threading
import logging

class TransactionWorker:

    all_queued_transactions = []
    queued_lock = threading.Lock()
    in_progress_transactions = []
    ip_lock = threading.Lock()

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    def run(self):
        for i, transaction in enumerate(self.transactions):
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            logging.debug("{}: completed transaction {} out of {}".format(threading.get_ident(), i, len(self.transactions)))

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

