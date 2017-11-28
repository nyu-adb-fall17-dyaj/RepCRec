from ticker import Ticker
from transaction import TransactionType,Transaction

class TransactionManager:
    def __init__(self,sites=None):
        self.sites=sites
        self.trxs = {}
        #wait_dict for each variable
        #transaction has list of pointer to wait for

    def begin(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_WRITE)
    
    def beginRO(self,trx):
        self.trxs[trx]=Transaction(trx,Ticker.get_tick(),TransactionType.READ_ONLY)

    def read(self,trx,var):
        pass

    def write(self,trx,var,val):
        pass

    def end(self,trx):
        pass

    def querystate(self):
        for t in self.trxs:
            self.trxs[t].querystate()