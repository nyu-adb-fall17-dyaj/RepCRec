from ticker import Ticker
from transaction import TransactionType,Transaction,TransactionStatus,Operation

class TransactionManager:
    def __init__(self,sites=None):
        self.sites=sites
        self.trxs = {}
        #waitlist is a list of name of waiting list order by time
        #retry waitlist when 
        #1. trx end / unavailble variable become available or abort
        #2. site recover
        #3. trx abort due to deadlock detection
        self.waitlist = []

    def begin(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_WRITE)
    
    def beginRO(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_ONLY)

    def read(self,trx,var):
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED:
            print('{} is already aborted'.format(trx))
            return
        potential_sites = self._locate_var(var)
        #list of trx block the read
        blocking_trx = None
        for s in potential_sites:
            #read success
            success,blocking_trx = s.read(t.id,t.type==TransactionType.READ_ONLY,t.timestamp,var)
            if success:
                print('Read success')
                return
            elif blocking_trx is not None:
                break
        #read fail
        print('Read fail')
        t.status = TransactionStatus.WAITING
        t.operation=Operation('r',var)
        self.waitlist.append(trx)
        #fail because some trx hold write lock on var
        
        if blocking_trx is not None:
            print('Blocked by {}'.format(blocking_trx))
            self.trxs[blocking_trx].blocked_trx.append(trx)
    

    
    def _locate_var(self,var):
        var_id = int(var[1:])
        if var_id%2==0:
            return self.sites.values()
        else:
            return [self.sites[1+(var_id%10)]]

    def write(self,trx,var,val):
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED:
            print('{} is already aborted'.format(trx))
            return

        potential_sites = self._locate_var(var)

        success = True
        blocking_trx = []
        for s in potential_sites:
            site_success,site_blocking_trx = s.write(trx,var,val)
            #if its blocked by site or not blocked, site_blocking_trx will be empty list
            blocking_trx.extend(site_blocking_trx)
            success= success and site_success
        
        #if success, var uncommited value will have been all updated
        if success:
            print('Write success')
        else:
            #blocked by other trx
            print('Write fail')
            if blocking_trx:
                print('Blocked by {}'.format(blocking_trx))
                for bt in blocking_trx:
                    self.trxs[bt].blocked_trx.append(trx)
                for s in potential_sites:
                    s.release_write_lock(trx,var)

            t.status = TransactionStatus.WAITING
            t.operation=Operation('w',var,val)
            self.waitlist.append(trx)
        


    def end(self,trx):
        pass

    def querystate(self):
        for t in self.trxs:
            self.trxs[t].querystate()