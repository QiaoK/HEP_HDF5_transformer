#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#define MAX_NAME 1024
#define ENABLE_MULTIDATASET 0
#define MULTIDATASET_DEFINE 0

#if MULTIDATASET_DEFINE == 1
typedef struct H5D_rw_multi_t
{
    hid_t dset_id;          /* dataset ID */
    hid_t dset_space_id;    /* dataset selection dataspace ID */
    hid_t mem_type_id;      /* memory datatype ID */
    hid_t mem_space_id;     /* memory selection dataspace ID */
    union {
        void *rbuf;         /* pointer to read buffer */
        const void *wbuf;   /* pointer to write buffer */
    } u;
} H5D_rw_multi_t;
#endif

int write_data(char *buf, hsize_t buf_size, char *dataset_name, hid_t out_id, hid_t mtype, H5D_rw_multi_t *dataset, char** attribute_names, char** attribute_bufs, hsize_t *attribute_sizes, hid_t *attribute_types, int n_attributes);
int fetch_data(hid_t did, char** buf, hsize_t *buf_size);
int scan_datasets(hid_t out_gid, hid_t gid, hid_t **dataset_list, size_t *dataset_list_size, size_t *dataset_list_max_size);
int clear_dataset (hid_t *dataset_list, hsize_t dataset_list_size);
int scan_attributes(hid_t hid, char*** attribute_names, char*** attribute_bufs, hsize_t** attribute_sizes, hid_t **attribute_types, int* n_attributes_ptr);
int flush_dataset(H5D_rw_multi_t *datasets, int dataset_size);

double dataset_write_time;
double metadata_write_time;
double open_time;
double close_time;

/*
 * This function is going to scan through an HDF5 file.
 * If gid is a group (could be the top directory"/"), this function is going to list all group and datasets.
 * For datasets, we are going to read them and write to our new file.
 * For groups, we are going to recurse into this function again.
*/
int scan_datasets(hid_t out_gid, hid_t gid, hid_t **dataset_list, size_t *dataset_list_size, size_t *dataset_list_max_size) {
    size_t i;
    hsize_t nobj, buf_size;
    int otype;
    hid_t grpid, out_grpid, dsid, did, tid;
    char group_name[MAX_NAME];
    char memb_name[MAX_NAME];
    hid_t *temp = NULL;
    char *buf = NULL;
    char **attribute_names = NULL, **attribute_bufs = NULL;
    hsize_t *attribute_sizes = NULL;
    hid_t *attribute_types = NULL;
    int n_attributes = 0;
    H5D_rw_multi_t *datasets = NULL;
    int dataset_index;

    H5Iget_name(gid, group_name, MAX_NAME  );
    H5Gget_num_objs(gid, &nobj);
    if (out_gid >= 0 && strcmp(group_name, "/") != 0) {
        out_grpid = H5Gcreate( out_gid, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
    } else if (strcmp(group_name, "/") == 0){
        out_grpid = out_gid;
    } else {
        out_grpid = -1;
    }
    datasets = (H5D_rw_multi_t*) malloc(sizeof(H5D_rw_multi_t) * nobj);
    dataset_index = 0;
    for (i = 0; i < nobj; i++) {
        H5Gget_objname_by_idx(gid, (hsize_t)i, memb_name, (size_t)MAX_NAME );
        otype =  H5Gget_objtype_by_idx(gid, (size_t)i );
        switch(otype) {
            case H5G_GROUP: {
                //printf(" GROUP: %s\n", memb_name);
                grpid = H5Gopen(gid, memb_name, H5P_DEFAULT);
                scan_datasets(out_grpid, grpid, dataset_list, dataset_list_size, dataset_list_max_size);
		scan_attributes(grpid, &attribute_names, &attribute_bufs, &attribute_sizes, &attribute_types, &n_attributes);
                n_attributes = 0;
                H5Gclose(grpid);
                break;
            }
            case H5G_DATASET: {
                //printf(" DATASET: %s\n", memb_name);KE
                /* Open a dataset from the current group */
                did = H5Dopen(gid, memb_name, H5P_DEFAULT);
		    //scan_attributes(did, &attribute_names, &attribute_bufs, &attribute_sizes, &attribute_types, &n_attributes);
                /* We write the dataset into our new file*/
		if ( out_grpid >= 0 ) {
                    dsid = H5Dget_space (did);
                    tid = H5Dget_type(did);
                    fetch_data(did, &buf, &buf_size);
		    write_data(buf, buf_size, memb_name, out_grpid, tid, datasets + dataset_index, attribute_names, attribute_bufs, attribute_sizes, attribute_types, n_attributes);
                    dataset_index++;
                    H5Sclose(dsid);
		    H5Tclose(tid);
		}

                if (!dataset_list_max_size[0]) {
                    dataset_list_max_size[0] = 128;
                    *dataset_list = (hid_t*) malloc(sizeof(hid_t) * dataset_list_max_size[0]);
                }
                if ( dataset_list_size[0] == dataset_list_max_size[0] ) {
                    dataset_list_max_size[0] *= 2;
                    temp = (hid_t*) malloc(sizeof(hid_t) * dataset_list_max_size[0]);
                    memcpy(temp, *dataset_list, sizeof(hid_t) * dataset_list_size[0]);
                    free(*dataset_list);
                    *dataset_list = temp;
                }
                dataset_list[0][dataset_list_size[0]++] = did;
                break;
            }
            default: {
                printf("something reached here\n");
                break;
            }
        }
    }
    flush_dataset(datasets, dataset_index);
    free(datasets);
    if ( out_gid >=0 && strcmp(group_name, "/") != 0) {
        H5Gclose(out_grpid);
    }
    return 0;
}

int scan_attributes(hid_t hid, char*** attribute_names, char*** attribute_bufs, hsize_t** attribute_sizes, hid_t **attribute_types, int* n_attributes_ptr) {
    hid_t aid, tid, asid;
    char attribute_name[1024];
    //H5A_info_t aid_info;
    int n_attributes = H5Aget_num_attrs(hid);
    int i;
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];
    int ndim;
    size_t esize;
    herr_t err;
    H5A_info_t ainfo;

    attribute_bufs[0] = (char**) malloc(sizeof(char*) * n_attributes);
    attribute_sizes[0] = (hsize_t*) malloc(sizeof(hsize_t) * n_attributes);
    attribute_names[0] = (char**) malloc(sizeof(char*) * n_attributes);
    attribute_types[0] = (hid_t*) malloc(sizeof(hid_t) * n_attributes);
    *n_attributes_ptr = n_attributes;

    for ( i = 0; i < n_attributes; ++i ) {
        aid = H5Aopen_by_idx( hid, ".", H5_INDEX_CRT_ORDER, H5_ITER_INC, (hsize_t) i, H5P_DEFAULT, H5P_DEFAULT );
        tid = H5Aget_type(aid);
	asid = H5Aget_space(aid);
	H5Aget_name(aid, 1024, attribute_name );

	attribute_names[0][i] = (char*) malloc(sizeof(char) * (strlen(attribute_name)+1));
	strcpy(attribute_names[0][i], attribute_name);
        ndim = H5Sget_simple_extent_dims(asid, dims, mdims );
	esize = H5Tget_size (tid);

	if ( ndim > 1 ){
            printf("This application does not support multi-dimensional metadata.\n");
        }
        //printf("metadata size = %llu, or %llu * %llu\n", (long long unsigned) aid_info.data_size, (long long unsigned) esize, (long long unsigned) dims[0] );


        H5Aget_info( aid, &ainfo );
	attribute_bufs[0][i] = (char*) malloc(ainfo.data_size * esize);
        attribute_types[0][i] = tid;
	attribute_sizes[0][i] = ainfo.data_size;
        printf("read attribute with name %s, size = %llu, esize = %llu, ndim = %d, dim size = %llu\n", attribute_name, (long long unsigned) ainfo.data_size, (long long unsigned)esize, ndim, (long long unsigned)dims[0] );
	err = H5Aread(aid, tid, attribute_bufs[0][i] );
	if (err < 0) {
	    printf("error code = %lld\n", (long long int) err);
	}
	H5Sclose(asid);
        H5Aclose(aid);
    }
    return 0;
}

int clear_dataset (hid_t *dataset_list, hsize_t dataset_list_size) {
    size_t i;
    for ( i = 0; i < dataset_list_size; ++i ) {
        H5Dclose(dataset_list[i]);
    }
    free(dataset_list);
    return 0;
}

/*
 * This function read a dataset into memory.
 * buf contains the actual data.
 * buf_size contains the number of data points in the dataset.
*/
int fetch_data(hid_t did, char** buf, hsize_t *buf_size) {
    hid_t dsid, sid, tid;
    int ndim, i;
    size_t esize;

    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];
    hsize_t total_data_size;

    dsid = H5Dget_space (did);

    tid = H5Dget_type(did);
    esize = H5Tget_size (tid);
    ndim = H5Sget_simple_extent_dims (dsid, dims, mdims);
    total_data_size = 1;
    for ( i = 0; i < ndim; ++i ) {
        total_data_size *= dims[i];
    }
    *buf = (char*) malloc(esize * total_data_size);
    *buf_size = total_data_size;
    dims[0] = total_data_size;
    mdims[0] = total_data_size;
    sid = H5Screate_simple (1, dims, mdims);

    H5Dread (did, tid, sid, dsid, H5P_DEFAULT, *buf);

    H5Sclose(dsid);
    H5Sclose(sid);
    H5Tclose(tid);

    return 0;
}

/*
 * Write data cache by H5D_rw_multi_t. We can either use multidataset or not.
*/

int flush_dataset(H5D_rw_multi_t *datasets, int dataset_size) {
    int i;
    void *temp;
    struct timeval start_time, end_time;

    gettimeofday(&start_time, NULL);
#if ENABLE_MULTIDATASET == 1
    hid_t dxplid = H5Pcreate (H5P_DATASET_XFER);
    printf("Multidataset: number of datasets to be written = %d\n", dataset_size);
    H5Dwrite_multi(dxplid, dataset_size, datasets);
    H5Pclose(dxplid);
#else
    printf("Independent write: number of datasets to be written = %d\n", dataset_size);
    for ( i = 0; i < dataset_size; ++i ) {
        H5Dwrite (datasets[i].dset_id, datasets[i].mem_type_id, datasets[i].dset_space_id, datasets[i].mem_space_id, H5P_DEFAULT, datasets[i].u.wbuf);
	H5Sclose (datasets[i].dset_space_id);
        H5Sclose (datasets[i].mem_space_id);
	H5Tclose (datasets[i].mem_type_id);
	H5Dclose (datasets[i].dset_id);
	temp = (void*) datasets[i].u.wbuf;
	free(temp);
    }
#endif
    gettimeofday(&end_time, NULL);
    dataset_write_time += (end_time.tv_usec + end_time.tv_sec * 1000000 - start_time.tv_usec - start_time.tv_sec * 1000000) * 1.0;
    return 0;
}

/*
 * Write a dataset. We cache the dataset information into the H5D_rw_multi_t structure. The flush_dataset function will take care of the actual write.
 * If there are any HDF5 attributes, we will take care of the write here.
*/
int write_data(char *buf, hsize_t buf_size, char *dataset_name, hid_t out_id, hid_t mtype, H5D_rw_multi_t *dataset, char** attribute_names, char** attribute_bufs, hsize_t *attribute_sizes, hid_t *attribute_types, int n_attributes) {
    hid_t sid, dsid, did, asid, aid;
    int i;
    struct timeval start_time, end_time;

    sid = H5Screate_simple (1, &buf_size, &buf_size);
    dsid = H5Screate_simple (1, &buf_size, &buf_size);
    gettimeofday(&start_time, NULL);
    did = H5Dcreate( out_id, dataset_name, mtype, dsid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
    gettimeofday(&end_time, NULL);
    open_time += (end_time.tv_usec + end_time.tv_sec * 1000000 - start_time.tv_usec - start_time.tv_sec * 1000000) * 1.0;

    gettimeofday(&start_time, NULL);
    //H5Dwrite (did, mtype, dsid, sid, H5P_DEFAULT, buf);
    dataset->dset_id = did;
    dataset->dset_space_id = dsid;
    dataset->mem_space_id = sid;
    dataset->u.wbuf = buf;
    dataset->mem_type_id = H5Tcopy(mtype);
    gettimeofday(&end_time, NULL);
    //dataset_write_time += (end_time.tv_usec + end_time.tv_sec * 1000000 - start_time.tv_usec - start_time.tv_sec * 1000000) * 1.0;

    gettimeofday(&start_time, NULL);
/*
    for ( i = 0; i < n_attributes; ++i ) {
        asid  = H5Screate_simple (1, attribute_sizes + i, attribute_sizes + i);
        aid = H5Acreate2( did, attribute_names[i], attribute_types[i], asid, H5P_DEFAULT, H5P_DEFAULT);
        H5Awrite(aid, attribute_types[i], attribute_bufs[i] );
        //printf("dataset to be written has size %llu, name = %s, aid = %lld\n", (long long unsigned) attribute_sizes[i], attribute_names[i], (long long int) aid);
	free(attribute_names[i]);
	free(attribute_bufs[i]);
	H5Tclose( attribute_types[i]);
	H5Sclose(asid);
	H5Aclose(aid);
    }
*/
    gettimeofday(&end_time, NULL);
    metadata_write_time += (end_time.tv_usec + end_time.tv_sec * 1000000 - start_time.tv_usec - start_time.tv_sec * 1000000) * 1.0;
    if ( n_attributes ) {
        free(attribute_names);
        free(attribute_bufs);
        free(attribute_sizes);
        free(attribute_types);
    }

    gettimeofday(&start_time, NULL);
    gettimeofday(&end_time, NULL);
    close_time += (end_time.tv_usec + end_time.tv_sec * 1000000 - start_time.tv_usec - start_time.tv_sec * 1000000) * 1.0;

    return 0;
}

int main (int argc, char **argv) {
    hid_t faplid, file, out_file, gid;
    size_t dataset_list_size = 0, dataset_list_max_size = 0;
    hid_t *dataset_list;
    char out_filename[256];

    if (argc != 2 ) { 
        printf("Usage: ./test filename\n");
        return 1;
    }

    printf("opening file %s\n", argv[1]);

    dataset_write_time = 0;
    metadata_write_time = 0;
    open_time = 0;
    close_time = 0;

    faplid = H5Pcreate (H5P_FILE_ACCESS);
    file = H5Fopen (argv[1], H5F_ACC_RDONLY, faplid);
    sprintf(out_filename, "%s.copy", argv[1]);

    //H5Pset_fapl_core( faplid, 10510925824, 1 );
    out_file = H5Fcreate (out_filename, H5F_ACC_TRUNC, H5P_DEFAULT, faplid);

    gid = H5Gopen(file, "/", H5P_DEFAULT);
    scan_datasets(out_file, gid, &dataset_list, &dataset_list_size, &dataset_list_max_size);

    clear_dataset(dataset_list, dataset_list_size);

    printf("open time = %lf\n", open_time/1000000);
    printf("metadata write time = %lf\n", metadata_write_time/1000000);
    printf("dataset write time = %lf\n", dataset_write_time/1000000);
    printf("close time = %lf\n", close_time/1000000);

    H5Gclose(gid);
    H5Pclose(faplid);
    H5Fclose(file);
    H5Fclose(out_file);
    return 0;
}
