
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
        ## USING REDUCE
        #store=pd.HDFStore('df_all.h5')
        
        from functools import reduce
        mylambda = lambda df,t:t.transform(df)


        with open('df_all.bin','wb') as f:                    
            for k in range(0,10):
                df = pd.DataFrame()
                df = reduce(mylambda, self.transforms[k:k+1],df) # 2 files for transfrom

                f.write(df.values.tobytes())
                del df # release memory
                #import gc
                #gc.collect()

                
        with open('df_all.bin','rb') as f:
            buffer = f.read()
            print("Loading data from buffer")
            data = np.transpose(np.frombuffer(buffer, dtype=np.float64).reshape(TRANSFORMS,ROWS))
            #df_all= pd.DataFrame(data=data, columns=list(range(TRANSFORMS)))

            print("Building da and ddf from numpy array")
            ddf = dd.from_array(da.from_array(data,chunks=5000))            
            #ddf = dd.from_array(data)
            mem_use()
        
        print("sleep for 5 seconds before returning df all")
        time.sleep(5)    
        exit()
        #df_all = ddf.compute()
        return df_all
        
if __name__ == '__main__':
    print(psutil.virtual_memory())    
    pipe = Pipeline()
    df = pipe.run()
    mem_use()    
    print("sleep for 5 seconds")
    time.sleep(5)
    
    print("** HERE IS WHERE THE TEST STARTS **")
    
    # Dont break the test
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
    print("TEST OK")
