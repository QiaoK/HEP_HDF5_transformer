# HEP_HDF5_transformer
+ This test read an existing HDF5 file. Then it will write out exactly the same datasets into a new file.
+ The overall logic is to iterate through all HDF5 group and datasets using the function int scan_datasets from HDF5 root file.
+ For HDF5 group (including the root group "/"), we recursively access datasets within it.
+ For HDF5 datasets, we stash data and flush them later.
+ Usage:
```
./test hdf5_outtest.h5 1
```
+ The first argument is the path to HDF5 file. The second argument is whether to use HDF5 core driver or not.
+ To run this test on Cori.
```
0. make
1. sbatch slurm.knl
```
+ You need to modify the Makefile for the compiler and the LD_LIBRARY_PATH in the slurm.knl.
