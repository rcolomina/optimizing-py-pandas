
import pandas as pd
import random
import psutil

random.seed(42)
 
TRANSFORMS = 10
ROWS = 20000000
DATA = [random.random() for _ in range(ROWS)]

def mem_use():
    mem_profile = psutil.virtual_memory()
    print("Memory Usage = {} | percent = {}".format(mem_profile.used,mem_profile.percent))


## RECURSIVELY APPLY PD CONCAT TO PAIRS OF DFs
def pd_concat(df1,df2):
    return pd.concat((df1,df2),axis=1)
#df0 = pd.DataFrame(DATA)    
#df1 = pd.DataFrame(DATA)
#mem_use()
#df01 = pd.concat((pd.concat((pd.concat((pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)),axis=1),pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)),axis=1)),axis=1),pd.concat((pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)),axis=1),pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)),axis=1)),axis=1)),axis=1),pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)),axis=1)),axis=1)
#df01 = pd.concat((pd.concat((pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA))),pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA))))),pd.concat((pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA))),pd.concat((pd.DataFrame(DATA),pd.DataFrame(DATA)))))))
#mem_use()


#df1 = pd.DataFrame(DATA)
#df2 = pd.DataFrame(DATA)
#df3 = pd.DataFrame(DATA)
mem_use()

# write df1 content in file.csv
#df1.to_csv('file.csv', index=False)
# append df2 content to file.csv
#df2.to_csv('file.csv', mode='a', columns=False, index=False)
# append df3 content to file.csv
#df3.to_csv('file.csv', mode='a', columns=False, index=False)
# mem_use()
# df4 = pd.DataFrame(DATA)
# mem_use()
# df5 = pd.DataFrame(DATA)
# mem_use()
# df6 = pd.DataFrame(DATA)
# mem_use()
# df7 = pd.DataFrame(DATA)
# mem_use()
# df8 = pd.DataFrame(DATA)
# mem_use()
# df9 = pd.DataFrame(DATA)
# mem_use()
# df10 = pd.DataFrame(DATA)
# mem_use()

import numpy as np
import pandas as pd
import os

mem_use()

# a DataFrame factory:
dfs=[]
for i in range(10):
    dfs.append(pd.DataFrame(DATA))

mem_use()

exit()   
# a csv solution
def bycsv(dfs):
    md,hd='w',True
    for df in dfs:
        df.to_csv('df_all.csv',mode=md,header=hd,index=None)
        md,hd='a',False
    #del dfs
    df_all=pd.read_csv('df_all.csv',index_col=None)
    os.remove('df_all.csv') 
    return df_all
dfall = bycsv(dfs)
print(dfall.info())
mem_use()
