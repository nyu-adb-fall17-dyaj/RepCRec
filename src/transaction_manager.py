from ticker import Ticker

class TransactionManager:
    def __init__(self,sites=None):
        self.sites=sites

    def begin(self,trx):
        pass
    
    def beginRO(self,trx):
        pass

    def read(self,trx,var):
        pass

    def write(self,trx,var,val):
        pass

    def end(self,trx):
        pass
        