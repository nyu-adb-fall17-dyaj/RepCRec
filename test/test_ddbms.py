import unittest
from src.ddbms import DDBMS
import sys
from io import StringIO

class TestDDBMS(unittest.TestCase):
    def setUp(self):
        self.result = StringIO()
        sys.stdout = self.result

    def test_RO_1(self):
        '''READ ONLY trx reads the latest commited value of variable before the time the trx starts'''
        db = DDBMS('test/test_RO_1')
        output = self.result.getvalue()
        self.assertIn('x2 has committed value 20',output)
        self.assertIn('x1 has committed value 10',output)
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T2'].status.name,'COMMITED')
        self.assertEqual(db.sites[2].vars['x1'].commited_value[0][1],101)
        for i in range(1,10):
            self.assertEqual(db.sites[i].vars['x2'].commited_value[0][1],102)

    def test_FIFO_lock_acquisition(self):
        '''Lock acquisition is first come first serve'''
        db = DDBMS('test/test_FIFO_lock_acquisition')
        output = self.result.getvalue()
        self.assertIn('x1 has committed value 10 modified at time 0',output)
        self.assertIn('x1 has committed value 5 modified at time 7',output)
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T2'].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T3'].status.name,'COMMITED')
    
    def test_commit_validation_1(self):
        '''Site fails after RW trx accesses, trx aborts when ends'''
        db = DDBMS('test/test_commit_validation_1')
        self.assertEqual(db.sites[2].vars['x1'].commited_value[0][1],10)
        self.assertEqual(db.tm.trxs['T1'].status.name,'ABORTED')
        
    def test_commit_validation_2(self):
        '''Site fails before RW trx tries to access, trx commits when ends'''
        db = DDBMS('test/test_commit_validation_2')
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')
        for i in range(1,10):
            if i == 2:
                self.assertEqual(db.sites[i].vars['x2'].commited_value[0][1],20)
            else:
                self.assertEqual(db.sites[i].vars['x2'].commited_value[0][1],1)
    
    def test_deadlock_detection_1(self):
        '''Detect and resolve single deadlock'''
        db = DDBMS('test/test_deadlock_detection_1')
        for i in range(1,5):
            self.assertEqual(db.tm.trxs['T'+str(i)].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T5'].status.name,'ABORTED')
    
    def test_deadlock_detection_2(self):
        '''Detect and resolve multiple deadlocks'''
        db = DDBMS('test/test_deadlock_detection_2')
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T3'].status.name,'COMMITED')
        self.assertEqual(db.tm.trxs['T2'].status.name,'ABORTED')
        self.assertEqual(db.tm.trxs['T4'].status.name,'ABORTED')

    def test_site_fail_recover_1(self):
        '''Non Replicated variable available to read immediately'''
        db = DDBMS('test/test_site_fail_recover_1')
        output = self.result.getvalue()
        self.assertIn('x1 has committed value 10 modified at time 0',output)
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')

    def test_site_fail_recover_2(self):
        '''Replicated variable available to read after new value commited'''
        db = DDBMS('test/test_site_fail_recover_2')
        output = self.result.getvalue()
        self.assertTrue(db.sites[1].vars['x2'].available_for_read)
        self.assertEqual(db.tm.trxs['T1'].status.name,'COMMITED')