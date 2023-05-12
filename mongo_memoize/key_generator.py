# -*- coding: utf-8 -*-

import sys
import hashlib

if sys.version_info[0] == 2:
    import cPickle as pickle
else:
    import pickle


class PickleMD5KeyGenerator(object):
    """Cache key generator using Pickle and MD5.

    :param int protocol: Pickle protocol version.
    """

    def __init__(self, protocol=-1):
        self._protocol = protocol

    def __call__(self, function_name, args, kwargs):
        pickled_args = pickle.dumps((function_name, args, sorted(kwargs.items())),
                               protocol=self._protocol)
        return hashlib.md5(pickled_args).hexdigest()
