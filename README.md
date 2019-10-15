# Optimising Pandas Concatenation

This project is a software analysis to investigate different ways to optimise concatenation of pandas data frames. For this analysis the following machine has been used:

* Memory: 7.7 GiB
* Processor: Intel Celeron CPU G 1840 @ 2.80Ghz x2
* OS type: 64 bit

This project has been focus on improving a given source code (see file "transform_original_code.py") due to it is not optimal in memory usage on forming pandas concatenation for its data frames. Thus, a memory profile was generated and dumped into a text file as starting point. This can be found in the text file "memory_profile_transform_original_code.txt".

## Original source

The original source code accumulates column data frames into a single big data frame producing an increasing of the memory during all the process. This leads to a memory overflow when many columns are appended. Its memory profile is found at the following text file:

* memory_profile_transform_original_code.txt

To execute the original source code type in the following command:

$ python transform_original_code.py


## Pre-Allocating Memory for the Final Data Frame

A first attempt of optimisation has been done making a pre-allocation of memory, which was quite reasonable idea for keeping constant the memory usege during the process of concatenation. This is potentially good idea because the final size of the pandas data frame is known from the very beginning. However, the results optained were shown that despite of a constant amount of memory was achieved, the figure was even higher than the maximum memory usage obtained from the original source code analysis. The memory profile of this approach is found at

* memory_profile_transform_df_preallocation.txt". 

To test this approach execute the following script:

$ python transform_df_preallocation.py

## Use of Lambda and Reduce in Data Frames Formation

A second attempt has been rewriting some parts of the code in a more functional way. A lambda function and a reduce method were used to encapsulate the aggregation of the data, from a empty pandas data frame into the final one. In this code re-written approach the performance obtained did not improve in memory usage beyond the original source code performance. Very similar performance was obtained saying that the original code and the rewritten in a functional manner both perform equaly. Thus, a last modification was applied. This was the usage of temporal files pickle files format dropped after each two columns concatenation. Later on these pickle will be reconstructed into the big final pursued. Indeed, this was a good idea, achieving a much more better memory performance than the original source code during the process of data frame formation. There was still a last peak of memory usage at the very precise moment to read back the temporary dropped pickle files again into memory. A memory profile of this method can be found at 

* memory_profile_transform_df_lambda_reduce.txt

To execute this approach type in the following command:

$ python transform_df_lambda_reduce.py

## Binary Buffer with Dark Data Frames

A third approach has been done using a binary buffer to store within a binary file all the data frames columns created during the process. Then a dark data frames has been used to produce the final data frame from the binary file in chunks. This approach has demonstrated a constant low memory usage during creating the binary buffer file, beating the original source code,  but there is still a peak of memory usage when all these pieces are bringing together into the final data frames. Its memory profile of the execution of this method is found at

* memory_profile_transform_""

Additionally to run this approach the installation of DASK is required. To install this use pip command as follows:

$ pip install -r requirements.txt

