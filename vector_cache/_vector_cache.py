'''
Created on Feb 6, 2014

@author: akittredge
'''
from functools import wraps
import pandas as pd
import numpy as np

def build_vector_cache(data_store):
    def vector_cache(user_function):
        '''Cache functions that build Pandas DataFrames.
        
        The decorator populates the required_data DataFrame with cached values and
        passes it to the decorated function.  The function should populate the
        N/A values in the DataFrame and return it.  The decorator will then write
        the new values to the cache.
        '''
        # When the function name or location changes you're going to have to re-cache everything, bummer.
        metric = '.'.join((user_function.__module__, user_function.__name__)) 
        @wraps(user_function)
        def wrapper(required_data_df):
            cached_data = data_store.get(metric=metric,
                                         df=required_data_df)
            missing_data = cached_data.isnull()
            missing_columns = missing_data.any(axis=0)
            missing_rows = missing_data.any(axis=1)
            if missing_columns.any() or missing_rows.any():
                missing_row_index = missing_rows[missing_rows == True].index
                missing_column_index = missing_columns.index[missing_columns == True]
                required_data = pd.DataFrame(index=missing_row_index,
                                             columns=missing_column_index)
                new_data = user_function(required_data=required_data)
                write_data = new_data.fillna(-np.inf)
                if write_data.any().any():
                    data_store.set(metric=metric, df=write_data)
                    cached_data.update(new_data)
            cached_data.replace(-np.inf, np.nan, inplace=True)
            return cached_data
        return wrapper
    return vector_cache
