class Variable:
    def __init__(self,id,val):
        self.id=id
        self.value=val

    def __str__(self):
        return '{}: {}'.format(self.id,self.value)