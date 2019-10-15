
"""
Task:
The peak memory usage of this script is too high and runs out of memory on users machines.
 
How can you fix the memory usage without breaking the test?
You should be able to relatively easily reduce the memory usage by _at least_ 25%
"""

from pandas.util import hash_pandas_object
import pandas as pd
import hashlib
import typing
import random
import time

import psutil
import sys
import numpy as np

from functools import reduce
import dask.array as da, dask.dataframe as dd

# Deterministic randomness for the assert at the end
random.seed(42)
 
TRANSFORMS = 10
ROWS = 20000000
#ROWS = 3
DATA = [random.random() for _ in range(ROWS)]

def mem_use():
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))

def update_df(df_orig: pd.DataFrame, df_new: pd.DataFrame, col) -> pd.DataFrame:
    # CONCAT ONTO DATAFRAME A NEW COLUMN
    return pd.concat((df_orig, df_new),axis=1)

class Transform:
    """A transform that adds a column of random data"""
 
    def __init__(self, var: str):
        self.var = var
 
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        mem_profile = psutil.virtual_memory()
        print("Transforming {} | Memory Usage = {} | percent = {}".format(self.var,
                                                                          mem_profile.used,
                                                                          mem_profile.percent))
        df_new = pd.DataFrame({self.var: DATA})
        return update_df(df, df_new,self.var)
        
class Pipeline:

    def __init__(self):        
        # CREATING LIST OF TRANSFORMERS
        self.transforms = [Transform(f'v{i}') for i in range(TRANSFORMS)]
        
    def run(self):        

        mylambda = lambda df,t:t.transform(df)

        df = reduce(mylambda, self.transforms[0:3],pd.DataFrame()) # only 1 transform is given
        print("writing df")
        #df.to_csv('df_column.csv')
        del df
        print("waiting 10 seconds to accomodate memory")
        time.sleep(10)
        mem_use()
        ddf = dd.read_csv('df_column.csv')
        mem_use()
        print("waiting 5 seconds after instanciated ddf")
        time.sleep(5)
        mem_use()
        exit()
        
        with open('df_all.bin','wb') as f:                    
            for k in range(0,10):
                df = reduce(mylambda, self.transforms[k:k+1],pd.DataFrame()) # only 1 transform is given
                f.write(df.values.tobytes()) # serialize data into buffer file
                del df # release memory

                
        with open('df_all.bin','rb') as f:
            mem_use()
            print("Building dask data array from binary file")
            dda = da.from_array(np.transpose(np.frombuffer(f.read(),
                                                           dtype=np.float64).reshape(TRANSFORMS,
                                                                                     ROWS)),
                                chunks=5000)
            mem_use()
            print("Creating dask data frame from dda")
            ddf = dd.from_array(dda)
            mem_use()
        
        return ddf
        
if __name__ == '__main__':
    print(psutil.virtual_memory())    
    pipe = Pipeline()
    df = pipe.run()
    mem_use()    
    print("Sleep another 5 seconds before test")
    time.sleep(5)    
    print("** HERE IS WHERE THE TEST STARTS **")
    df = df.compute() # tranforming dask data frame to pandas dataframe
    # Dont break the test
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
    print("TEST OK")
