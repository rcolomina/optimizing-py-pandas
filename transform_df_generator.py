
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

import time
# Deterministic randomness for the assert at the end
random.seed(42)
 
TRANSFORMS = 10
ROWS = 20000000
DATA = [random.random() for _ in range(ROWS)]

def mem_use():
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,
                                                    mem_profile.percent))

class Pipeline:

        
    def run(self):            
        mem_use()
        df = pd.DataFrame()
        def mygen():
            mem_use()
            for i in range(0,10):
                mem_use()
                yield pd.DataFrame({f'v{i}':np.array(DATA)})
        gen = mygen()
                
        return pd.concat(gen,axis=1,copy=False)
        
if __name__ == '__main__':
    print(psutil.virtual_memory())    
    pipe = Pipeline()
    df = pipe.run()    
    print("Data Frame was created")
    mem_use()
    print(df.info())
    print("HERE WHEN TEST STARTS (sleep 10 seconds)")
    time.sleep(10)
    # Dont break the test
    assert hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest() == '867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90'
