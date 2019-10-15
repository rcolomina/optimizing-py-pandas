
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
    # CONCAT TO BIG DATAFRAME A COLUMN
    return pd.concat([df_orig, df_new],
                     axis=1,
                     ignore_index=True,
                     sort=False,
                     copy=False)
 
class Transform:
    """A transform that adds a column of random data"""
 
    def __init__(self, var: str):
        self.var = var
 
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        
        # APPENDING
        #df = df.append(pd.DataFrame({self.var: DATA}))
        #print(df.head())
        #return df


        mem_profile = psutil.virtual_memory()
        print("Transforming {} | Memory Usage = {} | percent = {}".format(self.var,
                                                                          mem_profile.used,
                                                                          mem_profile.percent))
        df_new = pd.DataFrame({self.var: DATA})
        return update_df(df, df_new,self.var)

        
class Pipeline:

    def __init__(self):
        # INITIATING A VOID DATA FRAME
        self.df = pd.DataFrame()

        # CREATING LIST OF TRANSFORMERS
        self.transforms = [Transform(f'v{i}') for i in range(TRANSFORMS)]
        
    def run(self):
        ## USING FUNCTIONAL PROGRAMMING
        from functools import reduce
        mylambda = lambda df,t:t.transform(df)        
        self.df = reduce(mylambda, self.transforms,self.df)
        return self.df

    
        ## RUN WITH ON TRANSFORMS
        #for t in self.transforms:            
        #    self.df = t.transform(self.df)
        #    mem_profile = psutil.virtual_memory()
        #    print("Transforming {} | Memory Usage = {} | percent = {}".format(t.var,
        #                                                                      mem_profile.used,
        #                                                                      mem_profile.percent))
        #return self.df

    
        ## CONCATENATING A LIST
        #listDfs = []
        #for t in self.transforms:
        #    listDfs.append(pd.DataFrame({t.var:DATA}))
        #    mem_profile = psutil.virtual_memory()
        #    print(mem_profile)
        #    if (mem_profile.percent > 80):
        #        sys.exit()                    
        #self.df = pd.concat(listDfs,axis=1,ignore_index=True,sort=False,copy=False)                    
        #return self.df



        
if __name__ == '__main__':

    print(psutil.virtual_memory())    
    pipe = Pipeline()
    df = pipe.run()
    #print(df.head())
    #exit()
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))

    
    
    # Dont break the test
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
