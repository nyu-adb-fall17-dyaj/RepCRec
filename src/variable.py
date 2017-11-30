
class Variable:
    def __init__(self,id,val):
        self.id=id
        self.commited_value=[(0,val)]   #order by decreasing time
        self.uncommited_value=None
        self.available_for_read=True

    def querystate(self):
        print('{}: {} at time {}'.format(self.id,self.commited_value[0][1],self.commited_value[0][0]))

    def dump(self):
        print('{}: {} at time {}'.format(self.id,self.commited_value[0][1],self.commited_value[0][0]))

    def read(self,is_read_only,timestamp):
        if not self.available_for_read:
            print('{} not available for read yet'.format(self.id))
            return False
        if not is_read_only:
            if self.uncommited_value is not None:
                print('{} has unconmmited value {}'.format(self.id,self.uncommited_value))
            else:
                print('{} has conmmited value {} modified at time {}'.format(self.id,self.commited_value[0][1],self.commited_value[0][0]))
            return True
        else:
            for v in self.commited_value:
                if v[0]<timestamp:
                    print('{} has conmmited value {} modified at time {}'.format(self.id,v[1],v[0]))
                    return True
                
        #shouldn't reach here
        return False

    def write(self,val):
        self.uncommited_value=val
    
    def commit(self,time):
        print('commit value {} at time {}'.format(self.uncommited_value,time))
        self.commited_value.insert(0,(time,self.uncommited_value))
        self.uncommited_value=None
        self.available_for_read=True