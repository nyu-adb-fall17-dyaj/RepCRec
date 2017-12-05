'''Variable Stored In Database

This module provides the representation of variables in a database.

Authors:
    Da Ying (dy877@nyu.edu)
    Ardi Jusufi (aj2223@nyu.edu)
'''


class Variable:
    '''The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        id (str): Id of the variable
        commited_value ([(time(int),value(int))]): List of committed values of the variable in descending order of committed time
        uncommited_value (int): Value has not been committed
        available_for_read (bool): Flag indicates if a replicated variable is available for reading after site recovery
    '''

    def __init__(self, id, val):
        '''Inits a variable with id and the default value'''
        self.id = id
        self.commited_value = [(0, val)]  # order by decreasing time
        self.uncommited_value = None
        self.available_for_read = True

    def querystate(self):
        '''Print out the state of the variable'''
        print('{}: {} at time {}'.format(
            self.id, self.commited_value[0][1], self.commited_value[0][0]))

    def dump(self):
        '''Print out the state of the variable'''
        print('{}: {} at time {}'.format(
            self.id, self.commited_value[0][1], self.commited_value[0][0]))

    def read(self, is_read_only, timestamp):
        '''Read the value of the variable

        Returns correct value of the variable based on type and timestamp of the transaction

        Args:
            is_read_only (bool): Flag indicates if the transaction is READ ONLY transaction
            timestamp (int): Timestamp of the transaction

        Returns:
            Ture if read successfully, False otherwise

        '''

        # Replicated variable not available for read until first commit after
        # site recovers
        if not self.available_for_read:
            print('{} not available for read yet'.format(self.id))
            return False

        if not is_read_only:
            # READ WRITE transaction reads uncommitted value first if there is
            # one.
            if self.uncommited_value is not None:
                print('{} has uncommitted value {}'.format(
                    self.id, self.uncommited_value))
            else:
                print('{} has committed value {} modified at time {}'.format(
                    self.id, self.commited_value[0][1], self.commited_value[0][0]))
            return True
        else:
            # READ ONLY transaction reads latest committed value
            # whose committed time is earlier than the timestamp of the
            # transaction
            for v in self.commited_value:
                if v[0] < timestamp:
                    print('{} has committed value {} modified at time {}'.format(
                        self.id, v[1], v[0]))
                    return True

        # shouldn't reach here
        return False

    def write(self, val):
        '''Write value to uncommited value'''
        self.uncommited_value = val

    def commit(self, time):
        '''Commits the uncommitted value, and sets replicated variable to available for read.'''
        print('commit value {} at time {}'.format(self.uncommited_value, time))
        self.commited_value.insert(0, (time, self.uncommited_value))
        self.uncommited_value = None
        self.available_for_read = True
