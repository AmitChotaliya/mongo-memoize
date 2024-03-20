# flush_cache.py
import pymongo


class Flusher(object):
    def __init__(self, method, db_name='mongo_memoize', mongo_uri=None, mongo_client_cb=None,
                 collection_name="cache", prefix='memoize',connection_options={}, verbose=False) -> None:
        self.mongo_uri = mongo_uri
        self.connection_options = connection_options
        self.db_name = db_name

        self.collection_name = collection_name
        self.prefix = prefix
        self.verbose = verbose

        self.mongo_client_cb = mongo_client_cb
        self.db = None
        self.is_connected = False

        self.external_db_conn = True if mongo_client_cb else False

        self.qualname = str(method.__qualname__)

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

    def get_collection(self):
        '''Get cache collection object.'''
        col_name = self.collection_name
        cache_col = self.db[col_name]
        return cache_col

    def flush(self):
        '''Flush cache.'''
        cache_col = self.get_collection()
        deleted_cache = cache_col.delete_many({
            'qualname': self.qualname,
        })
        if self.verbose:
            print("flushed {} documents of {}".format(
                    deleted_cache.deleted_count,
                    self.qualname
                ))
        
def reset_cache(
    method, db_name='mongo_memoize', mongo_uri=None, mongo_client_cb=None,
    collection_name="cache",connection_options={}, verbose=False
):
    
    """ Global method to clear functional cache.
    
    Usage:

    >>> from mongo_memoize.reset import reset_cache
    >>> reset_cache(obj.method)
    ...

    :param str db_name: MongoDB database name.
    :param func mongo_client_cb: A function which returns MongoDB database connection as PyMongo Client    
    :param str mongo_uri: Mongodb Connection URI
    :param str collection_name: MongoDB collection name. If not specified, the
        collection name is generated automatically using the prefix, the module
        name, and the function name.
    :param dict connection_options: Additional parameters for establishing
        MongoDB connection."""
    
    flusher = Flusher(method, db_name=db_name, mongo_uri=mongo_uri, mongo_client_cb=mongo_client_cb,
                       collection_name=collection_name, connection_options=connection_options, verbose=verbose)
    
    flusher.connect()
    flusher.flush()
    flusher.disconnect()
