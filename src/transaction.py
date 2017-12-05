'''Database Transaction

This module provides the representation of transactions in a database.

Authors:
    Da Ying (dy877@nyu.edu)
    Ardi Jusufi (aj2223@nyu.edu)
'''

from enum import Enum


class TransactionType(Enum):
    '''Type of a transaction'''
    READ_ONLY = 0
    READ_WRITE = 1


class TransactionStatus(Enum):
    '''Status of a transaction'''
    RUNNING = 0
    WAITING = 1
    ABORTED = 2
    COMMITED = 3


class Operation:
    '''Operation of a transaction

    Represents a transaction operation

    Attributes:
        type (str): 'r' for read, 'w' for write
        var (str): variable to operate on
        val (int): value to write
    '''

    def __init__(self, o_type, var, val=None):
        '''Inits an operation'''
        self.type = o_type
        self.var = var
        self.val = val


class Transaction:
    '''Database Transaction

    Represents a transaction

    Attributes:
        id (str): Id of the transaction
        timestamp (int): Tick when the transaction begins
        type (TransactionType): Type of the transaction
        status (TransactionStatus): Status of the transaction
        wait_for (set(str)): Id of transactions blocking this transaction
        operation (Operation): Operation is waiting for this transaction
        site_access_time ({site(int):time(int)}): Earliest access time table for sites accessed
    '''

    def __init__(self, trx_id, timestamp, trx_type):
        '''Inits a transaction'''
        self.id = trx_id
        self.timestamp = timestamp
        self.type = trx_type
        self.status = TransactionStatus.RUNNING
        self.wait_for = set()
        self.operation = None
        self.site_access_time = {}  # site id: first success access tick

    def querystate(self):
        '''Print out the state of the transaction'''
        print('{}:'.format(self.id))
        print('started at time {}'.format(self.timestamp))
        print('type: {}'.format(self.type.name))
        print('status: {}'.format(self.status.name))
