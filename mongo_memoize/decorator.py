# -*- coding: utf-8 -*-

import pymongo
from functools import wraps
import hashlib

from key_generator import PickleMD5KeyGenerator
from serializer import PickleSerializer


def memoize(
    db_name='mongo_memoize', host='localhost', port=27017, collection_name=None,
    prefix='memoize', capped=False, capped_size=100000000, capped_max=None,
    connection_options={}, key_generator=None, serializer=None, verbose=False
):
    """A decorator that caches results of the function in MongoDB.

    Usage:

        >>> from mongo_memoize import memoize
        >>> @memoize()
        ... def some_function():
        ...     pass
        ...

    :param str db_name: MongoDB database name.
    :param str host: MongoDB host name.
    :param int port: MongoDB port.
    :param str collection_name: MongoDB collection name. If not specified, the
        collection name is generated automatically using the prefix, the module
        name, and the function name.
    :param str prefix: Prefix of the MongoDB collection name. This argument is
        only valid when the collection_name argument is not specified.
    :param bool capped: Whether to use the capped collection.
    :param int capped_size: The maximum size of the capped collection in bytes.
    :param int capped_max: The maximum number of items in the capped collection.
    :param dict connection_options: Additional parameters for establishing
        MongoDB connection.
    :param key_generator: Key generator instance.
        :class:`PickleMD5KeyGenerator <mongo_memoize.PickleMD5KeyGenerator>` is used by default.
    :param serializer: Serializer instance.
        :class:`PickleSerializer <mongo_memoize.PickleSerializer>` is used by default.
    """
    if not serializer:
        serializer = PickleSerializer()

    if not key_generator:
        key_generator = PickleMD5KeyGenerator()

    def decorator(func):
        db_conn = pymongo.MongoClient(host, port, *connection_options)[db_name]

        if collection_name:
            col_name = collection_name
        else:
            col_name = '%s_%s_%s' % (prefix, func.__name__, hashlib.md5(func.__module__).hexdigest())

        if capped:
            if col_name not in db_conn.collection_names():
                assert capped_size > 0, 'The size of the capped collection is required.'

                capped_args = {}
                capped_args['size'] = capped_size
                if capped_max:
                    capped_args['max'] = capped_max

                db_conn.create_collection(col_name, capped=True, **capped_args)

        cache_col = db_conn[col_name]
        cache_col.ensure_index('key', unique=True)

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            cache_key = key_generator(args, kwargs)

            cached_obj = cache_col.find_one(dict(key=cache_key))
            if cached_obj:
                if verbose:
                    print "Cache hit: {} ___ {}: {}".format(args, kwargs, cache_key)
                return serializer.deserialize(cached_obj['result'])

            if verbose:
                print "Cache miss: {} ___ {}".format(args, kwargs)

            ret = func(*args, **kwargs)
            cache_col.update(
                {'key': cache_key},
                {
                    '$set': {
                        'result': serializer.serialize(ret),
                        'args': str(args),
                        'kwargs': str(kwargs)
                    }
                },
                upsert=True
            )

            return ret

        return wrapped_func

    return decorator
