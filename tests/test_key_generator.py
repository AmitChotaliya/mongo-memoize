# -*- coding: utf-8 -*-

from mongo_memoize.key_generator import PickleMD5KeyGenerator

from nose.tools import *


def test_pickle_md5_key_generator():
    ins = PickleMD5KeyGenerator()
    key1 = ins("test_funct", (1, 2), dict(kwarg1=1))
    key1_2 = ins("test_funct", (1, 2), dict(kwarg1=1))
    key1_3 = ins("test_funct3", (1, 2), dict(kwarg1=1))
    ok_(isinstance(key1, str))
    ok_(key1_2 == key1)
    ok_(key1_2 != key1_3)

    key2 = ins("test_funct", (1,), dict(kwarg1=1))
    ok_(key1 != key2)

    key3 = ins("test_funct", (1, 2), dict(kwarg1=1, kwarg2=2))
    ok_(key1 != key3)
