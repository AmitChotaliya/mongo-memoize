import pymongo
from mongo_memoize import memoize
import unittest
from collections import defaultdict
from mongo_memoize import reset_cache


MONGO_URI = "mongodb://localhost"
DB_NAME = 'test'
call_count = defaultdict(int)

def get_db_conn():
    client = pymongo.MongoClient(MONGO_URI)
    return client

class Dashboard:
    def __init__(self) -> None:
        pass
    
    @memoize(db_name=DB_NAME, mongo_uri=MONGO_URI)
    def heavy_data(self, user=1):
        if call_count.get('heavy_data'):
            call_count['heavy_data'] += 1
        else:
            call_count['heavy_data'] = 1
        return True

@memoize(db_name=DB_NAME, mongo_uri=MONGO_URI)
def normal_function():
    if call_count.get('normal_function'):
        call_count['normal_function'] += 1
    else:
        call_count['normal_function'] = 1
    return True

class TestClearCache(unittest.TestCase):
    def setUp(self) -> None:
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db_name = "test"

    def tearDown(self) -> None:
        self.client.drop_database('test')
        self.client.close()
        
    def test_clear_cache(self):
        d1 = Dashboard()
        
        self.assertEqual(call_count['heavy_data'], 0)
        d1.heavy_data()
        self.assertEqual(call_count['heavy_data'], 1)
        
        # running again
        d1.heavy_data()
        self.assertEqual(call_count['heavy_data'], 1)
        
        # clearing cache
        reset_cache(d1.heavy_data, db_name=DB_NAME, mongo_client_cb=get_db_conn, verbose=False)

        d1.heavy_data()

        self.assertEqual(call_count['heavy_data'], 2)

    def test_normal_function(self):
        normal_function()
        
        self.assertEqual(call_count['normal_function'],1)
        normal_function()
        self.assertEqual(call_count['normal_function'],1)
        
        reset_cache(normal_function, db_name=DB_NAME, mongo_client_cb=get_db_conn, verbose=False)
        
        normal_function()
        self.assertEqual(call_count['normal_function'], 2)
