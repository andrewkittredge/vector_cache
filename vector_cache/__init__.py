'''
Created on Dec 3, 2013

@author: akittredge
'''




import sys
import _vector_cache
import sql_driver
import mongo_driver

class _VectorCache(object):
    '''strange class to deal with importing and timing.'''
    _vector_cache = _vector_cache
    sql_driver = sql_driver
    mongo_driver = mongo_driver
    @property
    def vector_cache(self):
        return self._vector_cache.build_vector_cache(get_data_store=self._get_data_store)
    
    @classmethod
    def _get_data_store(cls):
        '''User can specify data store in a file .vector_cacherc in their home directory.
        
        If there is no config file default to a sqlite db in their home directory.
        '''
        import imp, os
        
        home = os.path.expanduser("~")
        config_file_path = os.path.join(home, '.vector_cacherc')
        try:
            with open(config_file_path, 'rb') as code_file:
                code = code_file.read()
        except IOError:
            # no config in home directory
            return cls._default_data_store(db_dir=home)
        else:
            return cls._eval_config_code(code)
    
    @staticmethod
    def _default_data_store(db_dir):
        from sql_driver import SQLDataStore
        import os
        db_file_path = os.path.join(db_dir, 'vector_cache.db')
        connection_string = 'sqlite:///{}'.format(db_file_path)
        data_store = SQLDataStore(connection_string=connection_string)
        return data_store
    
    @staticmethod
    def _eval_config_code(code):
        import imp
        module = imp.new_module('vector_cache_config')
        exec code in module.__dict__
        data_store = module.data_store
        return data_store  

sys.modules[__name__] = _VectorCache()
