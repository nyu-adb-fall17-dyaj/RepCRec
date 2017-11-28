from ticker import Ticker
from variable import Variable

class DBSite:
    def __init__(self,id):
        self.id=id
        self.vars={}
        self.init_vars()
        self.up=True
        self.up_since=0
    
    def init_vars(self):
        for i in range(1,21):
            id = 'x'+str(i)
            val = 10 * i
            if i % 2 == 0:
                self.vars[id]=Variable(id,val)
            elif 1 + (i%10) == self.id:
                self.vars[id]=Variable(id,val)
    
    def __str__(self):
        s='Site {}:\n'.format(self.id)
        var_strs = [str(self.vars[v]) for v in self.vars]
        var_str = "\n".join(var_strs)
        return s+var_str+'\n'

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
