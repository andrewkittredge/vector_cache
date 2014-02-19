'''
Created on Jan 15, 2014

@author: akittredge
'''

import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker
Base = declarative_base()

class CachedValue(Base):
    __tablename__ = 'vector_cache'
    
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    metric = sqlalchemy.Column(sqlalchemy.String)
    date = sqlalchemy.Column(sqlalchemy.Date)
    identifier = sqlalchemy.Column(sqlalchemy.String)
    value = sqlalchemy.Column(sqlalchemy.Float)
    UniqueConstraint('metric', 'date', 'identifier')
    
class SQLDataStore(object):
    def __init__(self, connection_string):
        self._connection_string = connection_string
        engine = sqlalchemy.create_engine(connection_string)
        self._engine = engine
        CachedValue.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=engine)
        
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._connection_string)

    def get(self, metric, df):
        identifiers = list(df.columns)
        start, stop = df.index[0], df.index[-1]
        session = self._Session()
        records = session.query(CachedValue.date, 
                                CachedValue.identifier, 
                                CachedValue.value).\
                    filter(CachedValue.metric==metric).\
                    filter(CachedValue.identifier.in_(identifiers)).\
                    filter(CachedValue.date.between(start, stop))
        data_records = [rec.__dict__ for rec in records]
        # Should figure out how to get SQLAlchemy to do this.
        [record.pop('_labels') for record in data_records]
        store_data = pd.DataFrame.from_records(data_records)

        if not store_data.empty:
            store_data = store_data.pivot(index='date', 
                                          columns='identifier', 
                                          values='value')
            df.update(store_data)
        return df


    def set(self, metric, df):
        models = []
        for column in df:
            for index_value, value in df[column].iteritems():
                model = CachedValue(metric=metric,
                                    date=index_value,
                                    identifier=column,
                                    value=value)
                models.append(model)
        session = self._Session()
        session.add_all(models)
        session.commit()
