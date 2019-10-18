# Optimising Pandas Concatenation

This project is a software analysis to investigate different ways to optimise concatenation of pandas data frames. For this analysis the following machine has been used:

* Memory: 7.7 GiB
* Processor: Intel Celeron CPU G 1840 @ 2.80Ghz x2
* OS type: 64 bit

## Accumulating Columns Data Frames (Original Source Code Approach)

The original source code accumulates column pandas data frames into a single big pandas frame. The problem with this approach is coming from memory increase during all the process of the data frame construction when these are concatenated recursively column by column into a final big pandas data frame. Thus, this approach leads to a memory overflow when many columns need to be appended together. This method memory profile is found at the following text file:

* memory_profile_transform_original_code.txt

To execute the original source code type in the following command:

$ python transform_original_code.py


## Pre-Allocating Memory for the Final Data Frame

A first attempt of optimisation has been done making a pre-allocation of memory, which was quite reasonable idea for keeping constant the memory usege during the process of concatenation. This is potentially good idea because the final size of the pandas data frame is known from the very beginning. However, the results optained were shown that despite of a constant amount of memory was achieved, the figure was even higher than the maximum memory usage obtained from the original source code analysis. The memory profile of this approach is found at

* memory_profile_transform_df_preallocation.txt". 

To test this approach execute the following script:

$ python transform_df_preallocation.py

## Use of Lambda and Reduce in Data Frames Formation

A second attempt has been rewriting some parts of the code in a more functional way. A lambda function and a reduce method were used to encapsulate the aggregation of the data, from a empty pandas data frame into the final one. In this code re-written approach the performance obtained did not improve in memory usage beyond the original source code performance. Very similar performance was obtained saying that the original code and the rewritten in a functional manner both perform equaly. Thus, a last modification was applied. This was the usage of temporal files pickle files format dropped after each two columns concatenation. Later on these pickle will be reconstructed into the big final pursued. Indeed, this was a good idea, achieving a much more better memory performance than the original source code during the process of data frame formation. There was still a last peak of memory usage at the very precise moment to read back the temporary dropped pickle files again into memory. A memory profile is found within: 

* memory_profile_transform_df_lambda_reduce.txt

To execute this approach type in the following command:

$ python transform_df_lambda_reduce.py

## Binary Buffer File plus Dask Data Frames

A third approach was attempted using a single binary buffer to store all the columns data frames created during transformations. Afterwards, a dask data array is loaded from the latter binary file stored in disk, to produce in chunks a final dask data frame. This approach has demonstrated a constant low memory usage during the creation of intermediate data frame columns,beating the original source code;  but it's got still a peak of memory usage when all these pieces are bringing together into a final dask data frame. Its memory profile is found in the following text file:

* memory_profile_transform_df_buffer_dark.txt

Additionally to run this approach the installation of DASK is required. To install this use pip command as follows:

$ pip install -r requirements.txt

To execute this approach type in the following command:

$ python transform_df_buffer_dark.py 

## Using Generator as Source of Data Frame Concatenation

A fourth approach to minimise memory for data frame creation is using a generator as argument for "pd.concat" function.  This approach has shown that memory is increasing during the period slowly buy there is still a big peak eventually when all the columns are loaded into the final data frame. Its memory profile is found in the following text file:

* memory_profile_transform_df_generator.txt

To execute this approach type in the following command:

$ python transform_df_generator.py 


