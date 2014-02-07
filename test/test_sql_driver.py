'''
Created on Jan 16, 2014

@author: akittredge
'''
import unittest
from test.test_mongo_driver import DataStoreDriverTest
from vector_cache.sql_driver import SQLDataStore, CachedValue


class SQLiteDriverTestCase(DataStoreDriverTest, unittest.TestCase):
    metric = 'price'
    def build_data_store(self):
        self.data_store = SQLDataStore('sqlite:///:memory:')
        self.session = self.data_store._Session()
        return self.data_store, self.session
    
    def _populate_collection(self):
        super(SQLiteDriverTestCase, self)._populate_collection()
        for data_point in self.data_points:
            model = CachedValue(metric=self.metric,
                                date=data_point['date'],
                                identifier=data_point['identifier'],
                                value=data_point['price'])
            self.session.add(model)
        self.session.commit()

            
