# Optimising Pandas Concatenation

This project is a software analysis to investigate different ways to optimise concatenation of pandas data frames. For this analysis the following machine has been used:

* Memory: 7.7 GiB
* Processor: Intel Celeron CPU G 1840 @ 2.80Ghz x2
* OS type: 64 bit

## Method 0:  Accumulating Columns Data Frames (Original Source Code Approach)

The original source code accumulates column pandas data frames into a single big pandas frame. The problem with this approach is coming from memory increase during all the process of the data frame construction when these are concatenated recursively column by column into a final big pandas data frame. Thus, this approach leads to a memory overflow when many columns need to be appended together. This method memory profile is found at the following text file:

* memory_profile_transform_original_code.txt

To execute the original source code type in the following command:

$ python transform_original_code.py


## Method 1: Pre-Allocating Memory for the Final Data Frame

A first attempt of optimisation has been done making a pre-allocation of memory, which was quite reasonable idea for keeping constant the memory usege during the process of concatenation. This is potentially good idea because the final size of the pandas data frame is known from the very beginning. However, the results optained were shown that despite of a constant amount of memory was achieved, the figure was even higher than the maximum memory usage obtained from the original source code analysis. The memory profile of this approach is found at

* memory_profile_transform_df_preallocation.txt". 

To test this approach execute the following script:

$ python transform_df_preallocation.py

## Method 2: Use of Lambda and Reduce in Data Frames Formation

A second attempt has been rewriting some parts of the code in a more functional way. A lambda function and a reduce method were used to encapsulate the aggregation of the data, from a empty pandas data frame into the final one. In this code re-written approach the performance obtained did not improve in memory usage beyond the original source code performance. Very similar performance was obtained saying that the original code and the rewritten in a functional manner both perform equaly. Thus, a last modification was applied. This was the usage of temporal files pickle files format dropped after each two columns concatenation. Later on these pickle will be reconstructed into the big final pursued. Indeed, this was a good idea, achieving a much more better memory performance than the original source code during the process of data frame formation. There was still a last peak of memory usage at the very precise moment to read back the temporary dropped pickle files again into memory. A memory profile is found within: 

* memory_profile_transform_df_lambda_reduce.txt

To execute this approach type in the following command:

$ python transform_df_lambda_reduce.py

## Method 3: Binary Buffer File plus Dask Data Frames

A third approach was attempted using a single binary buffer to store all the columns data frames created during transformations. Afterwards, a dask data array is loaded from the latter binary file stored in disk, to produce in chunks a final dask data frame. This approach has demonstrated a constant low memory usage during the creation of intermediate data frame columns,beating the original source code;  but it's got still a peak of memory usage when all these pieces are bringing together into a final dask data frame. Its memory profile is found in the following text file:

* memory_profile_transform_df_buffer_dask.txt

Additionally to run this approach the installation of DASK is required. To install this use pip command as follows:

$ pip install -r requirements.txt

To execute this approach type in the following command:

$ python transform_df_buffer_dask.py 

## Method 4: Using Generator as Source of Data Frame Concatenation

A fourth approach to minimise memory for data frame creation is using a generator as argument for "pd.concat" function.  This approach has shown that memory is increasing during the period slowly buy there is still a big peak eventually when all the columns are loaded into the final data frame. Its memory profile is found in the following text file:

* memory_profile_transform_df_generator.txt

To execute this approach type in the following command:

$ python transform_df_generator.py 



# SUMMARY AND CONCLUSIONS

Compiling results from text memory profiles produced before it can be created a table summarising memory % in order to compare them each other. The methods that have been evaluated to minimise memory on pandas data frames concatenations are the followings 

* M-0 Accumulating Data Frames in Pairs
* M-1 Pre-Allocating the whole Data Frame
* M-2 Lambda and Reduce in formation
* M-3 Binary Buffering and Dask
* M-4 Providing Generator as input

At follows a summary of the memory profiles of these methods during pandas data frame creation.

| Memory %   | M-0 | M-1 | M-2 | M-3 | M-4 |
|  ---    | --- | --- | --- | --- | --- |
|  Iter 1 | 39  | 52  | 41  | 38  | 37  |
|  Iter 2 | 41  | 52  | 43  | 38  | 37  |
|  Iter 3 | 42  | 52  | 41  | 38  | 39  |
|  Iter 4 | 44  | 52  | 43  | 38  | 41  |
|  Iter 5 | 46  | 52  | 41  | 38  | 43  |
|  Iter 6 | 48  | 52  | 43  | 38  | 45  |
|  Iter 7   | 50  | 52  | 41  | 38  | 47  |
|  Iter 8    | 52  | 52  | 43  | 38  | 49  |
|  Iter 9      | 54  | 52  | 41  | 38  | 51  |
|  Iter 10       | 56 | 52  | 43  | 38  | 53 |
|  Post Creation | 57 | 52  | 59  | 57  | 60 | 

Within the testing machine the correspondence between percentage and memory usage for some numbers can be seen as follows:

| KB | GB    | Percentage |
| --- | ---    | ---        |
| 2718081024 | 2.592 | 38.1 |
| 4333924352 | 4.133 | 57.8 |

Memory measurements have been done using the python method psutil.virtual_memory() within the source code.


As conclusions can be observed M-1, M-2 and M-3 keep its memory usage constant during the process something that is very interesting, however its last peaks of memory persist similarly as M-2 and M-4. Despite of the array created should be stored in just 1.5 GB of memory, pandas data frame seems to uses more memory that that during the process of concatenations regardless whether this frame is created either from memory or from a file. 
Eventually, in order to keep a data frame of 1.5 GB in memory the systems seems to use much more than the actual size of the data frame something that will lead to memory overflow in low memory machines.
