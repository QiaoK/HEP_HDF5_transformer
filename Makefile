
MPICC		= cc
CFLAGS		= -O2 -Wall -Wextra

#H5_DIR          = /global/homes/q/qkt561/mpich_develop/hdf5-hdf5-1_12_0/install
H5_DIR           = /opt/cray/pe/hdf5-parallel/1.12.0.0/INTEL/19.1
#H5_DIR           = /global/homes/q/qkt561/mpich_develop/hdf5_multi/install

LDFLAGS        = -L$(H5_DIR)/lib -lhdf5
INCLUDES        = -I$(H5_DIR)/include

.c.o:
	$(MPICC) $(CFLAGS) $(INCLUDES) -c $<

all: test


test: hdf5_outputer_read.o
	$(MPICC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f core.* *.o test

.PHONY: clean

