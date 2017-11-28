from ticker import Ticker
from variable import Variable

class DBSite:
    def __init__(self,id):
        self.id=id
        self.vars={}
        self.init_vars()
        self.locktable={}
        self.init_locks()
        self.up=True
        self.up_since=0
    
    def init_locks(self):
        for v in self.vars:
            self.locktable[v]=[set(),None]     #[Read Locks, Write Lock]

    def acquire_read_lock(self,trx,var):
        if self.locktable[var][1] and self.locktable[var][1] != trx:
            return False
        self.locktable[var][0].add(trx)
        return True

    def acquire_write_lock(self,trx,var):
        readlocks,writelock = self.locktable[var]
        if (not readlocks or (trx in readlocks and len(readlocks)==1)) and (not writelock or writelock == trx):
            self.locktable[var][1]=trx
            return True
        return False

    def release_locks(self,trx):
        for k in self.locktable:
            if trx in self.locktable[k][0]:
                self.locktable[k][0].remove(trx)
            if trx == self.locktable[k][1]:
                self.locktable[k][1]=None

    def init_vars(self):
        for i in range(1,21):
            id = 'x'+str(i)
            val = 10 * i
            if i % 2 == 0:
                self.vars[id]=Variable(id,val)
            elif 1 + (i%10) == self.id:
                self.vars[id]=Variable(id,val)
    
    def querystate(self):
        print('**********Site {}:**********'.format(self.id))
        for v in self.vars:
            self.vars[v].querystate()
            print('locks: {}'.format(self.locktable[v]))

    def dump(self):
        if self.up:
            print('Site {}:'.format(self.id))
            for v in sorted(self.vars,key= lambda k:int(k[1:])):
                print(self.vars[v])
        else:
            print('Site {} is down'.format(self.id))

    def dump_var(self,var):
        if self.up and self.vars[var].available:
            print('Site {}:'.format(self.id))
            print(self.vars[var])
        elif self.up:
            print('{} not available yet'.format(var))
        else:
            print('Site {} is down'.format(self.id))
        

    def fail(self):
        self.up=False
    
    def recover(self):
        for v in self.vars:
            v_id = int(v[1:])
            
            if v_id%2==0:
                self.vars[v].available=False
            #unreplicated variable available immediately
            else:
                self.vars[v].available=True
        self.up=True
        self.up_since=Ticker.get_tick()
