from variable import Variable

class DBSite:
    def __init__(self,id):
        self.id=id
        self.init_vars()
    
    def init_vars(self):
        self.vars={}
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
