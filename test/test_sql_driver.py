'''
Created on Jan 16, 2014

@author: akittredge
'''
import unittest
from test.test_mongo_driver import DataStoreDriverTest
from vector_cache.sql_driver import SQLiteDataStore


class SQLiteDriverTestCase(DataStoreDriverTest, unittest.TestCase):
    table_name = 'vector_cache'
    metric = 'price'
    def build_data_store(self):
        self.connection = SQLiteDataStore.connect('/Users/akittredge/vector_cache.sql')
        self.data_store = SQLiteDataStore(connection=self.connection)
        return self.data_store, self.connection
    
    def _populate_collection(self):
        super(SQLiteDriverTestCase, self)._populate_collection()
        for data_point in self.data_points:
            qry = '''INSERT INTO {} (identifier, date, value, metric) VALUES (?, ?, ?, ?)'''
            qry = qry.format(self.table_name)
            args = data_point['identifier'], data_point['date'], data_point['price'], self.metric
            self.connection.execute(qry, args)