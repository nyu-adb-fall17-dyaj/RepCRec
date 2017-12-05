from .ticker import Ticker
from .variable import Variable

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

    def init_vars(self):
        for i in range(1,21):
            id = 'x'+str(i)
            val = 10 * i
            if i % 2 == 0:
                self.vars[id]=Variable(id,val)
            elif 1 + (i%10) == self.id:
                self.vars[id]=Variable(id,val)

    def acquire_read_lock(self,trx,var):
        if not self.vars[var].available_for_read:
            print('{} not available for read yet'.format(self.id))
            return (False,None)
        if self.locktable[var][1] and self.locktable[var][1] != trx:
            return (False,self.locktable[var][1])
        #if trx hold write lock, don't need to acquire new read lock
        if self.locktable[var][1] is None:
            self.locktable[var][0].add(trx)
        return (True,None)

    def acquire_write_lock(self,trx,var):
        readlocks,writelock = self.locktable[var]
        if (not readlocks or (trx in readlocks and len(readlocks)==1)) and (not writelock or writelock == trx):
            #if trx hold read lock, remove write lock, hold new write lock only
            if trx in readlocks:
                readlocks.remove(trx)
            self.locktable[var][1]=trx
            return (True,[])
        blocking_trx = []
        if len(readlocks)>0:
            blocking_trx.extend(list(readlocks))
        if writelock is not None:
            blocking_trx.append(writelock)
        if trx in blocking_trx:
            blocking_trx.remove(trx)
        return (False,blocking_trx)

    def abort(self,trx):
        #release locks
        #clear uncommited value of vars writen by trx
        for k in self.locktable:
            if trx in self.locktable[k][0]:
                self.locktable[k][0].remove(trx)
            if trx == self.locktable[k][1]:
                self.vars[k].uncommited_value = None
                self.locktable[k][1]=None
    
    def release_write_lock(self,trx,var):
        if trx == self.locktable[var][1]:
            self.locktable[var][1]=None

    def read(self,trx,is_read_only,timestamp,var):
        if not self.up:
            print('Site {} is down, try other sites'.format(self.id))
            return (False,None)
        #if READ WRITE trx, try acquire read lock
        if not is_read_only:
            success,blocking_trx = self.acquire_read_lock(trx,var)
            if not success:
                return (False,blocking_trx)
        return (self.vars[var].read(is_read_only,timestamp),None)

    def write(self,trx,var,val):
        if not self.up:
            print('Site {} is down, try next site'.format(self.id))
            return (False,[])
        
        success,blocking_trx = self.acquire_write_lock(trx,var)
        if not success:
            return (False,blocking_trx)
        self.vars[var].write(val)
        return (True,[])
        
    def commit(self,trx):
        for var in self.locktable:
            if self.locktable[var][1]==trx:
                print('{} in site {}'.format(var,self.id))
                self.vars[var].commit(Ticker.get_tick())
                self.locktable[var][1]=None
            if trx in self.locktable[var][0]:
                self.locktable[var][0].remove(trx)
    
    def querystate(self):
        print('**********Site {}:**********'.format(self.id))
        for v in self.vars:
            self.vars[v].querystate()
            #print('locks: {}'.format(self.locktable[v]))

    def dump(self):
        if self.up:
            print('Site {}:'.format(self.id))
            for v in sorted(self.vars,key= lambda k:int(k[1:])):
                self.vars[v].dump()
        else:
            print('Site {} is down'.format(self.id))

    def dump_var(self,var):
        if self.up and self.vars[var].available_for_read:
            print('Site {}:'.format(self.id))
            self.vars[var].dump()
        elif self.up:
            print('{} not available yet'.format(var))
        else:
            print('Site {} is down'.format(self.id))
        

    def fail(self):
        self.up=False
    
    def recover(self):
        self.init_locks()
        for v in self.vars:
            v_id = int(v[1:])
            
            if v_id%2==0:
                self.vars[v].available_for_read=False
            #unreplicated variable available immediately
            else:
                self.vars[v].available_for_read=True
        self.up=True
        self.up_since=Ticker.get_tick()
