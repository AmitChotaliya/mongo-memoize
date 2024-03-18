import pymongo
from mongo_memoize import memoize
import unittest
from collections import defaultdict
import time


MONGO_URI = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.1.5"
DB_NAME = 'test'
call_count = defaultdict(int)

def get_db_conn():
    client = pymongo.MongoClient(MONGO_URI)
    return client


class TestMemoize(unittest.TestCase):
    def setUp(self) -> None:
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db_name = "test"

    def tearDown(self) -> None:
        self.client.drop_database('test')
        self.client.close()

    def test_memoize_function_run_check(self):
        '''
        Check weather function runs properly
        '''
        self.assertEqual(call_count.get('memoize_function_run_check'), None)
        self.assertTrue(memoize_function_run_check())
        self.assertEqual(call_count['memoize_function_run_check'], 1)
        
        self.assertTrue(memoize_function_run_check())
        self.assertEqual(call_count['memoize_function_run_check'], 1)
        
        self.assertTrue(memoize_function_run_check())
        self.assertEqual(call_count['memoize_function_run_check'], 1)
        
    def test_mongodb_client_conn_arg(self):
        '''
        Check weather mongodb client conn arg runs properly
        '''
        self.assertEqual(call_count.get('check_mongodb_client_conn_arg'), None)
        self.assertTrue(check_mongodb_client_conn_arg())
        self.assertEqual(call_count['check_mongodb_client_conn_arg'], 1)
        self.assertTrue(check_mongodb_client_conn_arg())
        self.assertEqual(call_count['check_mongodb_client_conn_arg'], 1)
        
    def test_args_list(self):
        args = [1, 2, 3, 4]
        kwargs = dict()
        kwargs['first'] = 10
        kwargs['second'] = 30
        self.assertEqual(call_count.get('add_numbers'), None)
        self.assertEqual(add_numbers(*args, **kwargs), 50)
        self.assertEqual(call_count['add_numbers'], 1)
        self.assertEqual(add_numbers(*args, **kwargs), 50)
        self.assertEqual(call_count['add_numbers'], 1)
        
    def test_check_capped_max(self):
        '''
        Check for capped max value = 5
        '''
        for i in range(1, 11):
            self.assertEqual(check_capped_max(i), i)
            self.assertEqual(call_count['check_capped_max'], i)
        
        # Now the database only have capped results for 6 to 10
        # without capped_max it should only run 10 times
        for i in range(1, 6):
            self.assertEqual(check_capped_max(i), i)
            self.assertEqual(call_count['check_capped_max'], 10+i)
        self.assertEqual(call_count['check_capped_max'], 15)


    def test_check_max_age(self):
        '''check if max age works for the cache'''
        self.assertEqual(call_count['check_max_age'], 0)
        multiply(2, 3)
        self.assertEqual(call_count['check_max_age'], 1)
        
        # the cache is here
        multiply(2, 3)
        self.assertEqual(call_count['check_max_age'], 1)

        time.sleep(60)

        # Max age exceeded
        multiply(2, 3)
        self.assertEqual(call_count['check_max_age'], 2)


@memoize(db_name=DB_NAME, mongo_uri=MONGO_URI)
def memoize_function_run_check():
    if call_count.get('memoize_function_run_check'):
        call_count['memoize_function_run_check'] += 1
    else:
        call_count['memoize_function_run_check'] = 1
    return True


@memoize(db_name=DB_NAME, mongo_client_cb=get_db_conn)
def check_mongodb_client_conn_arg():
    if call_count.get('check_mongodb_client_conn_arg'):
        call_count['check_mongodb_client_conn_arg'] += 1
    else:
        call_count['check_mongodb_client_conn_arg'] = 1
    return True


@memoize(db_name=DB_NAME, mongo_client_cb=get_db_conn)
def add_numbers(*args, **kwargs):
    if call_count.get('add_numbers'):
        call_count['add_numbers'] += 1
    else:
        call_count['add_numbers'] = 1
    res = sum(args)
    for v in kwargs.values():
        res += int(v)
    return res


@memoize(db_name=DB_NAME, mongo_client_cb=get_db_conn, capped=True, capped_max=5)
def check_capped_max(a):
    if call_count.get('check_capped_max'):
        call_count['check_capped_max'] += 1
    else:
        call_count['check_capped_max'] = 1
    return a

@memoize(db_name=DB_NAME, mongo_client_cb=get_db_conn, max_age=10)
def multiply(a, b):
    if call_count.get('check_max_age'):
        call_count['check_max_age'] += 1
    else:
        call_count['check_max_age'] = 1
    return a * b


if __name__ == '__main__':
    unittest.main()
