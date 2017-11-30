from enum import Enum
class TransactionType(Enum):
    READ_ONLY = 0
    READ_WRITE = 1

class TransactionStatus(Enum):
    RUNNING = 0
    WAITING = 1
    ABORTED = 2
    COMMITED = 3

class Operation:
    '''
        type = READ/WRITE
        var = xi
        val = value
    '''
    def __init__(self,o_type,var,val=None):
        self.type=o_type
        self.var=var
        self.val=val

class Transaction:
    def __init__(self,trx_id,timestamp,trx_type):
        self.id = trx_id
        self.timestamp = timestamp
        self.type = trx_type
        self.status = TransactionStatus.RUNNING
        self.blocked_trx = []
        self.operation = None
        self.site_access_time = {}  #site id: first success access tick
    
    def querystate(self):
        print('{}:'.format(self.id))
        print('started at time {}'.format(self.timestamp))
        print('type: {}'.format(self.type.name))
        print('status: {}'.format(self.status.name))