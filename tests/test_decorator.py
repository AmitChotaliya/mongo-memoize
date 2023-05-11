# -*- coding: utf-8 -*-


import time
import uuid
from collections import defaultdict

from mongo_memoize.decorator import memoize
import pymongo
from nose.tools import *

call_count = defaultdict(int)
db_name = uuid.uuid1().hex
db_conn = pymongo.MongoClient()


def setup():
    db_conn.drop_database(db_name)


def tearDown():
    db_conn.drop_database(db_name)


@memoize(db_name=db_name)
def memoized_func():
    call_count['memoized_func'] += 1
    return True


@memoize(mongo_client=db_conn, db_name=db_name)
def memoized_func_with_db_conn():
    call_count['memoized_func_with_db_conn'] += 1
    return True


@memoize(db_name=db_name, collection_name='capped_col', capped=True,
         capped_size=1000, capped_max=10)
def memoized_func_capped():
    call_count['memoized_func_capped'] += 1
    return True


@with_setup(setup, tearDown)
def test_memoize():
    ok_(memoized_func())  # should be called
    time.sleep(1)
    ok_(memoized_func())
    eq_(1, call_count['memoized_func'])


@with_setup(setup, tearDown)
def test_memoize_with_db_conn():
    ok_(memoized_func_with_db_conn())  # should be called
    time.sleep(1)
    ok_(memoized_func_with_db_conn())
    eq_(1, call_count['memoized_func_with_db_conn'])


@with_setup(setup, tearDown)
def test_memoize_capped():
    ok_(memoized_func_capped())  # should be called
    time.sleep(1)
    ok_(memoized_func_capped())
    eq_(1, call_count['memoized_func_capped'])
