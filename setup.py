from setuptools import setup, find_packages

version = '0.1.0'
desc = '''\
vector_cache
=========================
:Author: Andrew Kittredge
:Version: $Revision: {}
:Copyright: Andrew Kittredge
:License: Apache Version 2

vector_cache caches pandas.DataFrames.  SQLite and Mongo datastores are implemented.

import vector_cache
import pandas as pd

stock_prices = pd.DataFrame(columns=['MSFT', 'GOOG'], 
			     index=pd.date_range('2012-1-1', '2012-12-31'))

@vector_cache.vector_cache
def closing_price(required_data):
    symbols = required_data.columns
    start, end = required_data.index[0], required_data.index[-1]
    new_data = get_stock_prices_from_third_party(symbols, start, end)
    required_data.update(new_data)
    return required_data

stock_prices = closing_price(required_data=stock_prices) # cache miss

stock_prices = closing_price(required_data=stock_prices) # cache hit

'''.format(version)

setup(name='vector_cache',
      version=version,
      description='caches pandas.DataFrames',
      long_description=desc,
      author='Andrew Kittredge',
      author_email='andrewlkittredge@gmail.com',
      license='Apache 2.0',
      packages=find_packages(),
      classifiers=[
	'Development Status :: 4 - Beta',
	'License :: OSI Approved :: Apache Software License',
	'Natural Language :: English',
	'Programming Language :: Python',
	'Programming Language :: Python :: 2.7',
	'Operating System :: OS Independent',
	'Intended Audience :: Science/Research',
     	],	
      install_requires=[
	'numpy',
	'pandas',
	'sqlalchemy',
	'pymongo',
	],
      url='https://github.com/andrewkittredge/vector_cache',
)
