from ticker import Ticker
from transaction import TransactionType, Transaction, TransactionStatus, Operation

from collections import defaultdict

class TransactionManager:
    def __init__(self, sites=None):
        self.sites = sites
        self.trxs = {}
        # waitlist is a list of name of waiting list order by time
        # retry waitlist when
        # 1. trx end / unavailble variable become available or abort
        # 2. site recover
        # 3. trx abort due to deadlock detection
        self.waitlist = []

    def begin(self, trx):
        self.trxs[trx] = Transaction(trx, Ticker.get_tick(), TransactionType.READ_WRITE)

    def beginRO(self, trx):
        self.trxs[trx] = Transaction(trx, Ticker.get_tick(), TransactionType.READ_ONLY)

    def retry_transaction(self):
        current_waitlist = self.waitlist[:]
        print('Retry {}'.format(current_waitlist))
        for trx in current_waitlist:
            o = self.trxs[trx].operation
            if o.type == 'r':
                self.read(trx, o.var)
            else:
                self.write(trx, o.var, o.val)

    def _wait_for_write(self,trx,var):
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
        print('Read {} for {}'.format(var, trx))
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED or t.status == TransactionStatus.COMMITED:
            print('{} is already aborted or commited'.format(trx))
            return
        
        # check if there write operation for same variable already waiting in front of this trx
        blocking_trx = self._wait_for_write(trx,var)

        # not write op for var before trx
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
        if(t.wait_for):
            print('Wait-for list: ', t.wait_for)

    def _locate_var(self, var):
        var_id = int(var[1:])
        if var_id % 2 == 0:
            return self.sites.keys()
        else:
            return [1 + (var_id % 10)]

    def write(self, trx, var, val):
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
        if(t.wait_for):
            print('Wait-for list: ', t.wait_for)

    def _remove_wait_for_edge(self, trx):
        '''
        Removes all waits-for edges leading to trx.
        I.e. if any transaction was waiting for trx to be done,
        at the end of this procedure it won't wait anymore.
        :param trx: Transaction that is being aborted/ended
        :return:
        '''
        for transaction, t in self.trxs.items():
            if(trx in t.wait_for):
                t.wait_for.remove(trx)


    def abort(self, trx):
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
        graph = defaultdict(list)
        for trx, t in self.trxs.items():
            graph[trx] = t.wait_for
        return graph

    def querystate(self):
        for t in self.trxs:
            self.trxs[t].querystate()