'''
Created on Dec 3, 2013

@author: akittredge
'''

import pymongo

from ._vector_cache import build_vector_cache
from .sql_driver import SQLDataStore


def get_data_store(host='localhost', port=27017):
    client = pymongo.MongoClient(host, port)
    collection = client.vector_cache.vector_cache
    import vector_cache.mongo_driver as vc_mongo_driver
    return vc_mongo_driver.MongoDataStore(collection=collection)

data_store = SQLDataStore(connection_string='sqlite:////tmp/test_vector_cache')
vector_cache = build_vector_cache(data_store=data_store)