'''
Created on Dec 3, 2013

@author: akittredge
'''
import unittest
import pandas as pd

import mock
from pandas.util.testing import assert_frame_equal
import vector_cache

class DataFrameCacheTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_data_store = mock.Mock()
        vector_cache._get_data_store = lambda : self.mock_data_store
        self.test_func = mock.Mock()
        self.test_func.__name__ = 'wrapped_func'
        self.decorated_func = vector_cache.vector_cache(self.test_func)

    def test_no_misses(self):
        '''When all required values are cached the decorated function should not be called.
        
        '''
        index = pd.date_range('2012-1-1', '2012-1-2')
        required_data = pd.DataFrame(columns=['a', 'b'], index=index)
        cache_data = pd.DataFrame({'a' : [1, 2], 'b' : [1, 2]})
        self.mock_data_store.get.return_value = cache_data
        ret_val = self.decorated_func(required_data)
        self.assertEqual(self.test_func.call_count, 0)
        assert_frame_equal(ret_val, cache_data)

    def test_column_miss(self):
        '''When required identifiers are missing from the cache they should be N/A's
        in the frame passed to the function.
        
        '''
        index = pd.date_range('2012-1-1', '2012-1-2')
        required_data = pd.DataFrame(columns=['a', 'b'], index=index)
        cached_data = required_data.copy()
        cached_data['a'] = [5, 6]
        self.mock_data_store.get.return_value = cached_data
        func_returns = pd.DataFrame({'b' : [7, 8]}, index=index)
        self.test_func.return_value = func_returns
        ret_val = self.decorated_func(required_data)
        miss_df = self.test_func.call_args[1]['required_data']
        assert_frame_equal(miss_df, pd.DataFrame(columns=['b'], index=index))
        exp = pd.DataFrame({'a' : [5, 6], 'b' : [7, 8]}, index=index)
        assert_frame_equal(ret_val, exp, check_dtype=False)
        save_df = self.mock_data_store.set.call_args[1]['df']
        assert_frame_equal(save_df, func_returns)
    
    def test_row_miss(self):
        '''When required index values are missing from the cache they should be N/A
        in the frame passed to the function.
        
        '''
        index = pd.date_range('2012-12-1', '2012-12-2')
        required_data = pd.DataFrame(index=index, columns=['a'])
        cached_data = required_data.copy()
        cached_data['a'] = [1, None]
        self.mock_data_store.get.return_value = cached_data
        func_returns = pd.DataFrame({'a' : [2]}, index=[index[1]])
        self.test_func.return_value = func_returns
        ret_val = self.decorated_func(required_data)
        miss_df = self.test_func.call_args[1]['required_data']
        assert_frame_equal(miss_df, pd.DataFrame(columns=['a'], index=[index[1]]))
        exp = pd.DataFrame({'a' : [1, 2]}, index=index)
        assert_frame_equal(ret_val, exp, check_dtype=False)
        save_df = self.mock_data_store.set.call_args[1]['df']
        assert_frame_equal(save_df, func_returns)
    
    def test_no_new_values(self):
        '''When the decorated function does not return new data the data_store
        should not be written to.
        
        '''
        pass