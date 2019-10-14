# Optimising Pandas Concatenation

This project is a software analysis investigating different ways to optimise concatenation of pandas data frames. For this analysis the following machine has been used:

* Memory: 7.7 GiB
* Processor: Intel Celeron CPU G 1840 @ 2.80Ghz x2
* OS type: 64 bit

This project has been focus on improving a given source code (see file "transform_original_code.py") due to it is not optimal in memory usage on forming pandas concatenation for its data frames. Thus, a memory profile was generated and dumped into a text file as starting point. This can be found in the text file "memory_profile_transform_original_code.txt".

A first attempt of optimisation has bene doing a pre-allocation of memory, which was quite reasonable idea for keeping constant the memory usege during all the process of concatenation. This is potentialy good idea due to the size of the final pandas data frame to configure was known beforehand. However, the results optained were shown that despite of a constant amount of memory was achieved, the figure was even higher than the maximum memory usage obtained from the original source code analysis. This source code can be found at "transform_df_preallocation.py" and a memory profiler in the text file "memory_profile_transfrom_df_preallocation.txt". To execute it the analysis execute the script:

$ python transform_df_preallocation.py

A second attempt was rewriting some parts of the code in a more functional way. A lambda function and a reduce method were used to encapsulate the aggregation of the data, from a empty pandas data frame into the final one. In this code re-written approach the performance obtained did not improve in memory usage beyond the original source code performance. Very similar performance was obtained saying that the original code and the rewritten in a functinal manner both perform equaly. Thus, a last modification was applied. This was the usage of temporal files, that were being dropped in pickle files format after each two  columns concatenation. Later on these pickle files will be reconstructed. Indeed, this was a good idea, allowing to achieve a much more better memory performance during the process of data frame formation than the original source code. There was still a last peak of memory usage at the moment to read back these dropped pickle files, again into memory in order to form the final big data frame. A memory profile of this method can be found at "memory_profile_transform_df_lambda_reduce.txt". To execute this variant:

$ python transform_df_lambda_reduce.py


