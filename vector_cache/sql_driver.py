'''
Created on Jan 15, 2014

@author: akittredge
'''
import sqlite3
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy.schema import UniqueConstraint
Base = declarative_base()

class CachedValue(Base):
    __tablename__ = 'vector_cache'
    
    id = Column(Integer, primary_key=True)
    metric = Column(String)
    date = Column(Date)
    identifier = Column(String)
    value = Column(Float)
    UniqueConstraint('metric', 'date', 'identifier')
    
    
class SQLDataStore(object):
    '''Implemented with SQLite in mind.'''
    _table_name = 'vector_cache'
    _index_name = 'date'
    def __init__(self, connection):
        self._connection = connection

    _get_query_template = ('SELECT * FROM {table_name}\n'
                           'WHERE {index} BETWEEN ? AND ?\n'
                           'AND metric = ?\n'
                           'AND identifier in ({placeholders})')
    def get(self, metric, df):
        identifiers = list(df.columns)
        start, stop = df.index[0], df.index[-1]
        # http://stackoverflow.com/questions/283645/python-list-in-sql-query-as-parameter
        placeholders = ', '.join('?' for _ in identifiers)
        qry = self._get_query_template.format(table_name=self._table_name,
                                              index=self._index_name,
                                              placeholders=placeholders)
        start = datetime.datetime(start.year, start.month, start.day)
        stop = datetime.datetime(stop.year, stop.month, stop.day)
        args = [start, stop, metric] + identifiers
        with self._connection:
            df = pd.io.sql.read_frame(qry, con=self._connection, params=args)
        if not df.empty:
            df = df.pivot(index='date', columns='identifier', values='value')
        return df
    
    _set_qry_temlate = ('INSERT INTO {table_name}\n'
                        '({metric}, identifier, {index}, value) VALUES (?, ?, ?, ?)')
    def set(self, metric, df):
        with self._connection:
            qry = self._set_qry_temlate(table_name=self._table_name,
                                        index=self._index_name)
            
        
        
class SQLiteDataStore(SQLDataStore):
    def __init__(self, connection):
        super(SQLiteDataStore, self).__init__(connection)
        with connection:
            qry = self.create_stmt.format(table_name=self._table_name)
            cursor = connection.cursor()
            cursor.execute(qry)
        
    create_stmt = ('CREATE TABLE IF NOT EXISTS {table_name}\n'
                   '(date timestamp, identifier text, metric text, value real)')
    @classmethod
    def connect(cls, database):
        detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES
        return sqlite3.connect(database, detect_types=detect_types)

    
import pytz
import datetime
def _tz_aware_timestamp_adapter(val):
    '''from https://gist.github.com/acdha/6655391'''
    datepart, timepart = val.split(b" ")
    year, month, day = map(int, datepart.split(b"-"))
 
    if b"+" in timepart:
        timepart, tz_offset = timepart.rsplit(b"+", 1)
        if tz_offset == b'00:00':
            tzinfo = pytz.utc
        else:
            hours, minutes = map(int, tz_offset.split(b':', 1))
            tzinfo = pytz.utc(datetime.timedelta(hours=hours, minutes=minutes))
    else:
        tzinfo = None
 
    timepart_full = timepart.split(b".")
    hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
 
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
    else:
        microseconds = 0
 
    val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds, tzinfo)
 
    return val
 
sqlite3.register_converter('timestamp', _tz_aware_timestamp_adapter)