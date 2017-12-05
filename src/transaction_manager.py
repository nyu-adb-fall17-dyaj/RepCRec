from .ticker import Ticker
from .transaction import TransactionType, Transaction, TransactionStatus, Operation

from collections import defaultdict

class TransactionManager:
    """Manages the transactions read from input, and dispatches them to respective classes for further handling.
       Authors: Da Ying and Ardi Jusufi"""
    def __init__(self, sites=None):
        """
        Maintains a list of sites and transactions used in the database, as well as a waitlist of all transactions
        waiting to finish execution.
        :param sites: Dictionary of sites to be used in the database indexed by number
        """
        self.sites = sites
        self.trxs = {}
        self.waitlist = [] # transactions waiting to execute ordered by time.

    def begin(self, trx):
        """
        Initializes a read/write transaction.
        :param trx: Transaction id being started, e.g. 'T1'
        """
        self.trxs[trx] = Transaction(trx, Ticker.get_tick(), TransactionType.READ_WRITE)

    def beginRO(self, trx):
        """
        Initializes a read-only transaction.
        :param trx: Transaction id being started, e.g. 'T1'
        """
        self.trxs[trx] = Transaction(trx, Ticker.get_tick(), TransactionType.READ_ONLY)

    def retry_transaction(self):
        """
        Retries all transactions in the waiting list ordered by time when:
        1. trx end / unavailable variable becomes available or aborts
        2. site recover, or
        3. trx abort due to deadlock detection
        """
        current_waitlist = self.waitlist[:]
        print('Retry {}'.format(current_waitlist))
        for trx in current_waitlist:
            o = self.trxs[trx].operation
            if o.type == 'r':
                self.read(trx, o.var)
            else:
                self.write(trx, o.var, o.val)

    def _wait_for_write(self, trx, var):
        """
        When a read/write trx wishes to acquire a read lock on var, it must ensure
        there is no other write transaction that is waiting to access it before trx.
        :param trx: r/w transaction seeking access to the variable
        :param var: variable that is sought to be read
        :return: transaction that is waiting to write on var ahead of trx, *if any*. Default is None.
        """
        current_waitlist = self.waitlist
        if trx in current_waitlist:
            index = current_waitlist.index(trx)
            current_waitlist = current_waitlist[:index]
        for t in current_waitlist:
            op = self.trxs[t].operation
            if op.type == 'w' and op.var == var:
                return t
        return None

    def read(self, trx, var):
        """
        Attempts to read a variable from any site where it is stored. If the read is not successful,
        the transaction is placed on the waitlist. It either prints the variable is read successfully,
        or displays a message if not.

        :param trx: Transaction attempting to read variable
        :param var: Variable being read
        """
        print('Read {} for {}'.format(var, trx))
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED or t.status == TransactionStatus.COMMITED:
            print('{} is already aborted or commited'.format(trx))
            return

        # check if there is a write operation for same variable already waiting in front of this trx:
        blocking_trx = self._wait_for_write(trx, var)

        # There is no write operation ahead of trx, so we can proceed with the read:
        if blocking_trx is None:
            potential_sites = self._locate_var(var)
            for s in potential_sites:
                # read success
                site = self.sites[s]
                success, blocking_trx = site.read(t.id, t.type == TransactionType.READ_ONLY, t.timestamp, var)
                if success:
                    print('Read success')
                    if trx in self.waitlist:
                        self.waitlist.remove(trx)
                    t.status = TransactionStatus.RUNNING
                    if t.type == TransactionType.READ_WRITE:
                        t.site_access_time[s] = Ticker.get_tick()
                    return
                elif blocking_trx is not None:
                    break

        # read fail
        print('Read fail')
        t.status = TransactionStatus.WAITING
        t.operation = Operation('r', var)
        if trx not in self.waitlist:
            self.waitlist.append(trx)
        # fail because some trx hold write lock on var

        if blocking_trx is not None:
            print('Blocked by {}'.format(blocking_trx))
            t.wait_for.add(blocking_trx)
        if (t.wait_for):
            print('Wait-for list: ', t.wait_for)

    def _locate_var(self, var):
        """
        Returns a list of sites where a variable is stored. If the variable is even, it is stored on all sites.
        If odd, it is stored on site with index 1 + (index % 10), e.g. x3 is stored on site 1 + (3 % 10) = 4.
        :param var: Variable whose site location is being requested
        :return: list of sites where the variable is stored
        """
        var_id = int(var[1:])
        if var_id % 2 == 0:
            return self.sites.keys()
        else:
            return [1 + (var_id % 10)]

    def write(self, trx, var, val):
        """
        Attempts to write a value on a variable on all active sites that store it. If the write is not successful,
        the transaction is placed on the waitlist. It either prints that the value was written successfully,
        or displays a message if not.

        :param trx: write transaction attempting to write value val on the variable var
        :param var: variable being updated
        :param val: value being written on variable
        """
        print('Write {} = {} for {}'.format(var, val, trx))
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED or t.status == TransactionStatus.COMMITED:
            print('{} is already aborted or commited'.format(trx))
            return

        potential_sites = self._locate_var(var)
        success_sites = []
        success = True
        blocking_trx = set()
        for s in potential_sites:
            site = self.sites[s]
            site_success, site_blocking_trx = site.write(trx, var, val)
            if site_success:
                success_sites.append(s)
            # if its blocked by site or not blocked, site_blocking_trx will be empty list
            blocking_trx.update(site_blocking_trx)
            # if site is down, doesn't mean the write will fail
            if not site_success and not site_blocking_trx:
                site_success = True
            success = success and site_success

        # if success, var uncommited value will have been all updated
        # success and there is site in success site
        # if all sites down, success sites will be empty
        if success and success_sites:
            print('Write success')
            if trx in self.waitlist:
                self.waitlist.remove(trx)
            t.status = TransactionStatus.RUNNING
            for s in success_sites:
                if s not in t.site_access_time:
                    t.site_access_time[s] = Ticker.get_tick()
        else:
            # blocked by other trx
            print('Write fail')
            if blocking_trx:
                print('Blocked by {}'.format(blocking_trx))
                t.wait_for.update(blocking_trx)
                for s in potential_sites:
                    self.sites[s].release_write_lock(trx, var)
            else:
                print('No available sites')
            t.status = TransactionStatus.WAITING
            t.operation = Operation('w', var, val)
            if trx not in self.waitlist:
                self.waitlist.append(trx)
        if (t.wait_for):
            print('Wait-for list: ', t.wait_for)

    def _remove_wait_for_edge(self, trx):
        '''
        Removes all waits-for edges leading to transaction trx.
        I.e. if any transaction was waiting for trx to be done,
        at the end of this procedure it won't wait anymore.
        This is needed for solving and preventing deadlocks.

        :param trx: Transaction that is being aborted/ended
        '''
        for transaction, t in self.trxs.items():
            if (trx in t.wait_for):
                t.wait_for.remove(trx)

    def abort(self, trx):
        """
        Aborts transaction trx and retries transactions on the waitlist,
        because any transaction that was waiting for trx can now proceed.

        :param trx: transaction being aborted
        """
        if trx in self.waitlist:
            self.waitlist.remove(trx)
        print('{} aborted'.format(trx))
        self.trxs[trx].status = TransactionStatus.ABORTED

        if self.trxs[trx].type == TransactionType.READ_WRITE:
            for s in self.sites:
                self.sites[s].abort(trx)
            self._remove_wait_for_edge(trx)
            self.retry_transaction()

    def end(self, trx):
        """
        Attempts to commit transaction trx if possible.
        :param trx: transaction trying to be committed
        """
        if trx in self.waitlist:
            self.abort(trx)
            return
        t = self.trxs[trx]
        if t.type == TransactionType.READ_ONLY:
            print('READ ONLY {} commited'.format(trx))
            t.status = TransactionStatus.COMMITED
            return
        t_site_access_time = t.site_access_time

        for s in t_site_access_time:
            if t_site_access_time[s] < self.sites[s].up_since or not self.sites[s].up:
                print(
                    'Site {}, accessed by {} at {}, but up since {} or still down'.format(s, trx, t_site_access_time[s],
                                                                                          self.sites[s].up_since))
                self.abort(trx)
                return
        # pass validation
        for s in t_site_access_time:
            self.sites[s].commit(trx)
        self._remove_wait_for_edge(trx)
        t.status = TransactionStatus.COMMITED
        print('READ WRITE {} commited'.format(trx))
        self.retry_transaction()

    def _generate_waits_for_graph(self):
        """
        Generates a waits-for graph of all transactions, which is used for cycle detection
        in resolving deadlocks.

        :return: dictionary which for each transaction trx
                 maintains a list of other transactions that trx is waiting for to finish
        """
        graph = defaultdict(list)
        for trx, t in self.trxs.items():
            graph[trx] = t.wait_for
        return graph

    def querystate(self):
        """
        Calls and subsequently prints the state of transactions.
        """
        for t in self.trxs:
            self.trxs[t].querystate()