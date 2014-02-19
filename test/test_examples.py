'''
Created on Jan 12, 2014

@author: akittredge
'''
import unittest
import pandas as pd

import os
import pickle
import mock
from test.test_data_store_drivers import memory_sql_store
import vector_cache


TEST_DOCS_DIR = os.path.join(os.path.dirname(__file__), 'assets')
with open(os.path.join(TEST_DOCS_DIR, 'treasury_data.pkl'), 'rb') as f:
    treasury_data = pickle.load(f)

class TestTreasuries(unittest.TestCase):
    def setUp(self):
        self.data_store, _ = memory_sql_store()
        vector_cache._get_data_store = lambda : self.data_store

    @mock.patch('zipline.data.treasuries.get_treasury_data', return_value=treasury_data)
    def test_treasury_caching(self, *args, **kwargs):
        from examples.treasuries import treasury_yield
        required_data = pd.DataFrame(columns=['1year', '10year'], 
                                     index=pd.date_range('2012-12-1', '2012-12-31'))
        treasury_yield(required_data)
        
        data = treasury_yield(required_data)
        self.assertAlmostEqual(data['1year'][2], .018, delta=.001)
        