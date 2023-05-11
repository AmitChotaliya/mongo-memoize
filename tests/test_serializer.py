# -*- coding: utf-8 -*-

from mongo_memoize.serializer import NoopSerializer, PickleSerializer

from nose.tools import *


def test_noop_serializer():
    ins = NoopSerializer()
    obj = dict(a=1, b=2)

    serialized = ins.serialize(obj)
    eq_(obj, serialized)

    deserialized = ins.deserialize(serialized)
    eq_(obj, deserialized)


def test_pickle_serializer():
    ins = PickleSerializer()
    obj = dict(a=1, b=2)

    serialized = ins.serialize(obj)
    ok_(isinstance(serialized, (bytes, bytearray)))

    deserialized = ins.deserialize(serialized)
    eq_(obj, deserialized)
