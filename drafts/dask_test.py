from pandas.util import hash_pandas_object
import pandas as pd
import hashlib
import typing
import random

import pandas as pd, numpy as np 
import dask.array as da, dask.dataframe as dd
import random

random.seed(42)

TRANSFORMS = 10
ROWS = 20000000
DATA = [random.random() for _ in range(ROWS)]

def men_use():
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))


    
c1 = da.from_array(np.array(DATA),chunks=5000) #np.arange(100000, 190000), chunks=1000)

#lData = [da.from_array(DATA,chunks=10000) for _ in range(10)]

# generate dask dataframe
ddf = dd.concat([dd.from_dask_array(c) for c in [c1 for _ in range(10)]], axis = 1) 
# name columns
#ddf.columns = ['c1', 'c2', 'c3']

#print(ddf.head())
#df = ddf.compute()

#assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'

