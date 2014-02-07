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


if __name__ == '__main__':
    required_data = pd.DataFrame(columns=['1year', '10year'], 
                                 index=pd.date_range('2012-1-1', '2012-12-31'))
    treasuries = treasury_yield(required_data)
    print treasuries