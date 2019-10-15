
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

import psutil
import sys
import numpy as np

# Deterministic randomness for the assert at the end
random.seed(42)
 
TRANSFORMS = 10
ROWS = 20000000
DATA = [random.random() for _ in range(ROWS)]

def update_df(df_orig: pd.DataFrame, df_new: pd.DataFrame, col) -> pd.DataFrame:
    # CONCAT ONTO DATAFRAME A NEW COLUMN
    return pd.concat((df_orig, df_new),
                     axis=1,
                     ignore_index=True,
                     sort=False,
                     copy=False)
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
        from functools import reduce
        mylambda = lambda df,t:t.transform(df)

        def write_df(df,num):
            df.to_pickle("/tmp/data_frame_df"+str(num)+".pickle")        

        for k in range(0,5):
            df = pd.DataFrame()
            df = reduce(mylambda, self.transforms[2*k:2*k+2],df) # 2 files for transfrom
            write_df(df,k) # write df on file 
            del df # release memory
                    
        df_from_pickles = dd.concat([pd.read_pickle("/tmp/data_frame_df"+str(i)+".pickle") for i in range(0,5)],
                                    axis=1)

        mem_profile = psutil.virtual_memory()
        print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                        mem_profile.percent))
        
        return df_from_pickles
        
if __name__ == '__main__':
    print(psutil.virtual_memory())    
    pipe = Pipeline()
    df = pipe.run()
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))
    print(df.info())
    # Dont break the test
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
