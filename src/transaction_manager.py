from ticker import Ticker
from transaction import TransactionType,Transaction,TransactionStatus,Operation

class TransactionManager:
    def __init__(self,sites=None):
        self.sites=sites
        self.trxs = {}
        #waitlist is a list of name of waiting list order by time
        #retry waitlist when 
        #1. trx end or abort
        #2. site recover
        #3. unavailble variable become available
        self.waitlist = []

    def begin(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_WRITE)
    
    def beginRO(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_ONLY)

    def read(self,trx,var):
        t = self.trxs[trx]
        if t.status == TransactionStatus.ABORTED:
            print('{} is already aborted'.format(trx))
        potential_sites = self._locate_var(var)
        #list of trx block the read
        blocking_trx = None
        for s in potential_sites:
            #read success
            success,blocking_trx = s.read(t.id,t.type==TransactionType.READ_ONLY,t.timestamp,var)
            if success:
                return
            elif blocking_trx is not None:
                break
        #read fail
        t.status = TransactionStatus.WAITING
        t.operation=Operation('r',var)
        self.waitlist.append(trx)
        #fail because some trx hold write lock on var
        self.trxs[blocking_trx].blocked_trx.append(trx)

    
    def _locate_var(self,var):
        var_id = int(var[1:])
        if var_id%2==0:
            return self.sites.values()
        else:
            return [self.sites[1+(var_id%10)]]

    def write(self,trx,var,val):
        pass

    def end(self,trx):
        pass

    def querystate(self):
        for t in self.trxs:
            self.trxs[t].querystate()