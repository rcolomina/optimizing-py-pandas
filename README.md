# Optimising Pandas Concatenation

This project tries to investigate different ways to optimise concatenation of pandas data frames.  

The goal of this project is try to improve a pre-existed source code "transform_original_code.py" because it was not optimal. Thus, a memory profile was done and dumped into a text file: "memory_profile_transform_original_code.txt".

As first attempt of optimisation a pre-allocation of memory was a feasible idea to keep constant the memory used during the process. This is reasonable due to the size of the final pandas data frame was known beforehand. However, the results have shown that despite there is a constant amount of memory achieved, this figure is even higher than the maximum memory usage that has been obtained from the original source code analysis. This source code can be found at "transform_df_preallocation.py" and a memory profiler at "memory_profile_transfrom_df_preallocation.txt"

To execute it:

$ python transform_df_preallocation.py

As second attempt has been rewriting some parts of the code in a more functional way. A lambda function and a reduce method were used to encapsulate the aggregation of data from a empty data frame into the final one. This re-written approach was used, but its performance did not improve memory usage further than the original source code. Exactly the same performance. The last option found was using temporal files dropped in pickle format each 2 columns concatenation. Later on these pickle files will be reconstructed. Indeed, this has been a good idea allowing to achieve a much more better memory performance during the process of data frame formation. There was still a last peak of memory usage at the moment to read back these pickle files again into memory returning the final data frame concatenated. A memory profile of this method can be found at "memory_profile_transform_df_lambda_reduce.txt". To execute this variant:

$ python transform_df_lambda_reduce.py


