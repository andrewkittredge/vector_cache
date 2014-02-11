vector_cache
======================

Caching for functions that return pandas DataFrames.

	@vector_cache
	def foo(empty_df):
	    print 'populating df'
	    results = lengthy_calculation(df.index, df.columns)
	    empty_df.update(results)
	    return empty_df

	required_data = pandas.DataFrame(index=[1, 2, 3, 4], columns=['a, 'b', 'c'])
	foo(required_data) # foo is called
	foo(required_data) #foo is not called
