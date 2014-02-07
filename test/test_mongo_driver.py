'''
Created on Jan 9, 2014

@author: akittredge
'''
import unittest

import pandas as pd
import datetime
import vector_cache.mongo_driver as vc_mongo_driver
import pymongo
from pandas.util.testing import assert_frame_equal
import operator
import abc

TEST_HOST, TEST_PORT = 'localhost', 27017

class DataStoreDriverTest(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def build_data_store(self):
        return self.data_store, self.collection
    
    data_points = [
           {'identifier' : 'a',
                        'date' : datetime.datetime(2012, 12, 1),
                        'price' : 1},
           {'identifier' : 'a',
                        'date' : datetime.datetime(2012, 12, 2),
                        'price' : 2},
           {'identifier' : 'a',
                        'date' : datetime.datetime(2012, 12, 3),
                        'price' : 3},
           {'identifier' : 'b',
                        'date' : datetime.datetime(2012, 12, 1),
                        'price' : 100},
           {'identifier' : 'b',
                        'date' : datetime.datetime(2012, 12, 4),
                        'price' : 110},
           ]
    @abc.abstractmethod
    def _populate_collection(self):
        self.test_df = pd.DataFrame(self.data_points).pivot(index='date',
                                                            columns='identifier',
                                                            values='price')
        self.index = self.test_df.index
        '''put data_points into data_store'''
    
    def setUp(self):
        self.data_store, self.collection = self.build_data_store()
        self._populate_collection()

    def test_get(self):
        '''Get previously cached values.'''
        empty_df = pd.DataFrame(columns=['a', 'b'], index=self.index)
        empty_df.columns.name = 'identifier'
        df_from_datastore = self.data_store.get(metric='price',
                                                df=empty_df)
        assert_frame_equal(df_from_datastore, self.test_df, check_dtype=False)
        
        
class MongoDriverTestCase(DataStoreDriverTest, unittest.TestCase):
    def test_read_frame(self):
        '''read a Dataframe from mongo.'''
        df = vc_mongo_driver.read_frame(qry={},
                                        index='date',
                                        values='price',
                                        collection=self.collection,
                                        )
        assert_frame_equal(df, self.test_df)
        
    def test_no_cached_values(self):
        '''when no cache values are found an empty DataFrame should be returned.'''
        df = vc_mongo_driver.read_frame(qry={'this is not in the cache' : True},
                                        index='date',
                                        values='price',
                                        collection=self.collection,
                                        )
        self.assertTrue(df.empty)
    
    def test_write_frame(self):
        '''write a DataFrame to mongo.'''
        metric = 'metric'
        collection = self.collection
        collection.drop()
        vc_mongo_driver.write_frame(metric=metric, 
                                    df=self.test_df,
                                    collection=collection)
        records = list(collection.find())
        self.assertEqual(len(records), operator.mul(*self.test_df.shape))
        
    def _populate_collection(self):
        super(MongoDriverTestCase, self)._populate_collections()
        for data_point in self.data_points:
            self.collection.insert(data_point)

        
    @classmethod
    def build_data_store(cls, host=TEST_HOST, port=TEST_PORT, collection_name='test'):
        client = pymongo.MongoClient(host, port)
        db = client.test
        collection = db[collection_name]
        collection.drop()
        data_store = vc_mongo_driver.MongoDataStore(collection=collection)
        return data_store, collection