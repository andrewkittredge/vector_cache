'''
Created on Jan 12, 2014

@author: akittredge
'''
import unittest
import pandas as pd
from examples.treasuries import treasury_yield
import os
import pickle
import mock
from test.test_mongo_driver import build_data_store

TEST_DOCS_DIR = os.path.join(os.path.dirname(__file__), 'assets')
with open(os.path.join(TEST_DOCS_DIR, 'treasury_data.pkl'), 'rb') as f:
    treasury_data = pickle.load(f)
    pass

class TestTreasuries(unittest.TestCase):
    
    def setUp(self):
        data_store, collection = build_data_store()
        self.data_store = data_store
        
        
    @mock.patch('zipline.data.treasuries.get_treasury_data', return_value=treasury_data)
    @mock.patch('vector_cache.get_data_store')
    def test_treasury_caching(self, get_data_store, *args, **kwargs):
        get_data_store.return_value = self.data_store
        required_data = pd.DataFrame(columns=['1year', '10year'], 
                                     index=pd.date_range('2012-12-1', '2012-12-31'))
        treasury_yield(required_data)
        
        data = treasury_yield(required_data)
        print data