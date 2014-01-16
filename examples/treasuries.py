'''
Created on Nov 29, 2013

@author: akittredge
'''
import pandas as pd
import zipline.data.treasuries as zipline_treasuries
from vector_cache import vector_cache


@vector_cache
def treasury_yield(required_data):
    '''
    required data is an empty? dataframe that will be populated.
    
    
    Returns a dataframe.
    '''
    treasury_data = zipline_treasuries.get_treasury_data()
    treasury_yield = pd.DataFrame.from_records(data=treasury_data)
    treasury_yield.set_index('date', inplace=True)
    required_data.update(treasury_yield)
    return required_data
