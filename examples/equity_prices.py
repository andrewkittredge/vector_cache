'''
Created on Feb 8, 2014

@author: akittredge
'''
from vector_cache import vector_cache
from pandas.io.data import get_data_yahoo

@vector_cache
def get_prices_from_yahoo(required_data, type_of_price='Adj Close'):
    '''Yields date, value pairs.'''
    # Yahoo errors out if you only ask for recent dates.
    start, end = required_data.index[0], required_data.index[-1]
    symbols = list(required_data.columns)
    new_data = get_data_yahoo(symbols=symbols, start=start, end=end)
    required_data.update(new_data.Close)
    return required_data


if __name__ == '__main__':
    import pandas as pd
    required_data = pd.DataFrame(columns=['GOOG', 'YHOO', 'MSFT'], 
                                 index=pd.date_range('2012-1-1', '2012-12-31'))
    prices = get_prices_from_yahoo(required_data)
    print prices