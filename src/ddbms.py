from ticker import Ticker
from variable import Variable
from dbsite import DBSite
from transaction_manager import TransactionManager

class DDBMS:
    def __init__(self,inputfile):
        self.inputf = open(inputfile)
        self.sites = {}
        self.init_site()
        self.tm = TransactionManager(self.sites)
        self.querystate()
        self.run()
        self.querystate()

    def init_site(self):
        for i in range(1,11):
            self.sites[i]=DBSite(i)

    def querystate(self):
        print('----------System State at Time {}'.format(Ticker.get_tick()))
        for s in self.sites:
            self.sites[s].querystate()
        print('~~~~~~~~~~Transactions~~~~~~~~~~')
        self.tm.querystate()

    def run(self):
        print('Start')
        for line in self.inputf:
            line = line.split(')')[0]
            line = line.split('(')
            method = line[0]
            args = line[1].split(',')
            print('----------Tick {}----------'.format(Ticker.get_tick()))
            getattr(self,method)(*args)
            Ticker.next_tick()
        print('Done')

    def begin(self,trx):
        print('{} begins'.format(trx))
        self.tm.begin(trx)    

    def beginRO(self,trx):
        print('RO {} begins'.format(trx))
        self.tm.beginRO(trx)

    def R(self,trx,var):
        print('Read {} for {}'.format(var,trx))
        self.tm.read(trx,var)

    def W(self,trx,var,val):
        print('Write {} to {} for {}'.format(var,val,trx))
        self.tm.write(trx,var,val)

    def dump(self,arg):
        if not arg:
            print('Dump all')
            self._dump_all()
        elif 'x' in arg:
            print('Dump value of {}'.format(arg))
            self._dump_var(arg)
        else:
            print('Dump values of all variables in site {}'.format(arg))
            self._dump_site(arg)

    def _dump_all(self):
        for s in self.sites:
            self.sites[s].dump()
    
    def _dump_var(self,var):
        sites = self._locate_var(var)
        for s in sites:
            s.dump_var(var)

    def _dump_site(self,site):
        self.sites[int(site)].dump()

    def _locate_var(self,var):
        var_id = int(var[1:])
        if var_id%2==0:
            return self.sites.values()
        else:
            return [self.sites[1+(var_id%10)]]

    def end(self,trx):
        print('{} ends'.format(trx))
        self.tm.end(trx)

    def fail(self,site):
        print('Site {} fails'.format(site))
        self.sites[int(site)].fail()

    def recover(self,site):
        print('Recover site {}'.format(site))
        self.sites[int(site)].recover()

if __name__ == '__main__':
    ddbms = DDBMS('input')