'''
Created on Jul 28, 2013

@author: akittredge
'''

import pandas as pd
import pymongo

       
class MongoDataStore(object):
    def __init__(self, collection):
        self._collection = collection
        
    def __repr__(self):
        return '{}(collection={})'.format(self.__class__.__name__, 
                                          self._collection.full_name)
        
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('index_val', pymongo.ASCENDING), 
                                 ('identifier', pymongo.ASCENDING)])
        
    def get(self, metric, df):
        '''Populate a DataFrame.
        
        '''
        identifiers = list(df.columns)
        start, stop = df.index[0], df.index[-1]
        index = 'date'
        metric = self.sanitize_key(metric)
        query = {'identifier' : {'$in' : identifiers},
                 metric : {'$exists' : True},
                 index : {'$gte' : start,
                          '$lte' : stop},
                 }
        store_data = read_frame(qry=query,
                                index=index,
                                values=metric,
                                collection=self._collection)
        df.update(store_data)
        return df

    def set(self, metric, df):
        metric = self.sanitize_key(metric)
        write_frame(metric=metric,
                    df=df,
                    collection=self._collection)
        
    @classmethod
    def sanitize_key(cls, key):
        '''Can't have . or $ in mongo field names.'''
        key = key.replace('.', unichr(0xFF0E))
        key = key.replace('$', unichr(0xFF04))
        return key
        
# after pandas.io.sql

def read_frame(qry, index, values, collection):
    documents = collection.find(qry)
    result = pd.DataFrame.from_records(documents)
    if not result.empty:
        result = result.pivot(index=index, columns='identifier', values=values)
    return result

def write_frame(metric, df, collection):
    docs = []
    index_name = 'date'
    for column in df:
        for index_value, value in df[column].iteritems():
            docs.append({'identifier' : column,
                         index_name : index_value,
                         metric : value})
    collection.insert(docs)
