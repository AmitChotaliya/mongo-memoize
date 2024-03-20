# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import pymongo
from functools import wraps

from mongo_memoize.key_generator import PickleMD5KeyGenerator
from mongo_memoize.serializer import PickleSerializer

import datetime

class Memoizer(object):

    def __init__(self, db_name='mongo_memoize', mongo_client_cb=None, mongo_uri=None, collection_name=None,
                 prefix='memoize', capped=False, capped_size=100000000, capped_max=None, max_age=None,
                 connection_options={}, key_generator=None, serializer=None, verbose=False, timeout=0):

        self.serializer = serializer
        if not self.serializer:
            self.serializer = PickleSerializer()

        self.key_generator = key_generator
        if not self.key_generator:
            self.key_generator = PickleMD5KeyGenerator()

        self.mongo_uri = mongo_uri
        self.connection_options = connection_options
        self.db_name = db_name

        self.collection_name = collection_name
        self.prefix = prefix
        self.capped = capped
        self.capped_size = capped_size
        self.capped_max = capped_max
        self.verbose = verbose
        self.timeout = timeout
        self.max_age = max_age

        self.mongo_client_cb = mongo_client_cb
        self.db = None
        self.is_connected = False
        self.external_db_conn = True if mongo_client_cb else False

    def connect(self):
        if self.external_db_conn:
            self.db_conn = self.mongo_client_cb()
        else:
            self.db_conn = pymongo.MongoClient(
                self.mongo_uri, *self.connection_options)        
        self.db = self.db_conn[self.db_name]
        self.is_connected = True

    def disconnect(self):
        if not self.external_db_conn:
            try:
                self.db_conn.close()
            except AttributeError:
                self.db_conn = None
            self.is_connected = False

    def initialize_col(self, func):
        col_name = self.collection_name

        if self.capped:
            if col_name not in self.db.list_collection_names():
                assert self.capped_size > 0, 'The size of the capped collection is required.'

                capped_args = dict()
                capped_args['size'] = self.capped_size
                if self.capped_max:
                    capped_args['max'] = self.capped_max

                self.db.create_collection(col_name, capped=True, **capped_args)

        cache_col = self.db[col_name]
        cache_col.create_index('key', unique=True)

        if self.max_age is not None:
            # if the document db supports it or not.
            cache_col.create_index('expiresAt', expireAfterSeconds=0)

        return cache_col

    @staticmethod
    def normalize_args_list(arg_list, kwarg_list):
        if arg_list is None and kwarg_list is None:
            return list()

        if arg_list is None:
            arg_list = [() for _ in kwarg_list]

        if kwarg_list is None:
            if len(arg_list) and isinstance(arg_list[0], dict):
                raise ValueError("Dictionary provided in arg_list.  If only kwarg_list is to be provide it, "
                                 "provide it as a named argument.")
            kwarg_list = [dict() for _ in arg_list]

        return arg_list, kwarg_list


def memoize(
        db_name='mongo_memoize', mongo_uri=None, mongo_client_cb=None, collection_name="cache",
        prefix='memoize', capped=False, capped_size=100000000, capped_max=None, max_age=None,
        connection_options={}, key_generator=None, serializer=None, verbose=False, timeout=0
):
    """A decorator that caches results of the function in MongoDB.

    Usage:

        >>> from mongo_memoize import memoize
        >>> @memoize()
        ... def some_function():
        ...     pass
        ...

    :param str db_name: MongoDB database name.
    :param func mongo_client_cb: A function which returns MongoDB database connection as PyMongo Client    
    :param str mongo_uri: Mongodb Connection URI
    :param str collection_name: MongoDB collection name. If not specified, the
        collection name is generated automatically using the prefix, the module
        name, and the function name.
    :param str prefix: Prefix of the MongoDB collection name. This argument is
        only valid when the collection_name argument is not specified.
    :param bool capped: Whether to use the capped collection.
    :param int capped_size: The maximum size of the capped collection in bytes.
    :param int capped_max: The maximum number of items in the capped collection.
    :param max_age: The maximum age of the cached item in seconds.
    :param dict connection_options: Additional parameters for establishing
        MongoDB connection.
    :param key_generator: Key generator instance.
        :class:`PickleMD5KeyGenerator <mongo_memoize.PickleMD5KeyGenerator>` is used by default.
    :param serializer: Serializer instance.
        :class:`PickleSerializer <mongo_memoize.PickleSerializer>` is used by default.
    """

    def decorator(func):

        memoizer = Memoizer(db_name, mongo_client_cb=mongo_client_cb, mongo_uri=mongo_uri, collection_name=collection_name,
                            prefix=prefix, capped=capped, capped_size=capped_size, capped_max=capped_max, max_age=max_age,
                            connection_options=connection_options, key_generator=key_generator,
                            serializer=serializer, verbose=verbose, timeout=timeout)
        

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            memoizer.connect()
            cache_col = memoizer.initialize_col(func)
            cache_key = memoizer.key_generator(
                func.__module__.encode('utf-8'), args, kwargs)
            cached_obj = cache_col.find_one(dict(key=cache_key))
            if cached_obj:
                if verbose:
                    print("Cache hit: {} ___ {}".format(args, kwargs))
                return memoizer.serializer.deserialize(cached_obj['result'])

            if verbose:
                print("Cache miss: {} ___ {}".format(args, kwargs))

            ret = func(*args, **kwargs)

            resultSet = {
                'result': memoizer.serializer.serialize(ret),
                'qualname': str(func.__qualname__),
                'args': str(args),
                'kwargs': str(kwargs),
            }

            if max_age is not None:
                resultSet['expiresAt'] = datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=max_age)

            cache_col.update_one(
                    {'key': cache_key},
                    {
                        '$set': resultSet
                    },
                    upsert=True
                )

            memoizer.disconnect()

            return ret

        return wrapped_func

    return decorator
