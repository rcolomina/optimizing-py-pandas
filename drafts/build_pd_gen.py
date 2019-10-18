from pandas.util import hash_pandas_object
import hashlib

import pandas as pd
import random

random.seed(42)

import numpy as np
import psutil
import time
    
ROWS = 20000000
DATA = [random.random() for _ in range(ROWS)]

def mem_use():
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))

def apply_assertion(df):
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
    
## CREATE AND LOAD ARRAYS IN MEMORY
def using_numpy_arrays():
    larray = []
    for i in range(0,10):
        mem_use()
        larray.append(np.array(DATA))  #// 2.19  GB

    del larray    

    time.sleep(5)  


### USING HDF Storage
def using_hdf_storage():
    
    store = pd.HDFStore('store.h5')

    for i in range(0,10):    
        mem_use()  ## constant memory usage (start with 
        store[f'v{i}'] = pd.DataFrame({f'v{i}':DATA})

    df = pd.DataFrame()
    generat = ( store[f'v{i}'] for i in range(0,10)) 
    df = pd.concat(generat,axis=1)
    
    mem_use()
    print(df.head())
    time.sleep(5)  

using_hdf_storage()    
exit()    
import gc
    
# USING GENERATOR
def create_df_using_generator():
    
    genera = ( pd.DataFramex for x in range(1,20000000))
    mem_use()

    myDf = pd.DataFrame()
    myDf = pd.concat(genera,axis=1)
    mem_use()
    
    print(myDf.dtypes)
    print(myDf.tail(10))
    mem_use()

    apply_assertion(myDf)
    time.sleep(5)


#using_hdf_storage()
#create_df_using_generator()

def create_df_iloc():
    labels = [f'v{i}' for i in range(0,10)]
    df = pd.DataFrame(index=range(0,ROWS),columns=range(0,10))
    print(df.memory_usage())
    gc.enable()
    
    for index, colLabel in enumerate(labels):
        df.iloc[:,index] = np.array(DATA)
        print(df.memory_usage())

    print(df.head())
    df.memory_usage()
          
#create_df_iloc()

def df_using_generator_final():
    mem_use()
    df = pd.DataFrame()
    mygen = (pd.DataFrame(np.array(DATA)) for _ in range(0,10))
    df = pd.concat(mygen,axis=1)
    mem_use()
    #for i in range(0,10):
    #    df = pd.concat((df,pd.DataFrame(np.array(DATA))),axis=1)
    #    print(df.memory_usage())
    #apply_assertion(df)
          
df_using_generator_final()

