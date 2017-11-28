class Variable:
    def __init__(self,id,val):
        self.id=id
        self.commited_value=[(0,val)]
        self.uncommited_value=None
        self.available=True

    def querystate(self):
        print('{}: {} at time {}'.format(self.id,self.commited_value[-1][1],self.commited_value[-1][0]))