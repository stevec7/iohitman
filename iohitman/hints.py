from mpi4py import MPI

def set_mpi_file_info(striping_factor=1, striping_unit=4194304):
    mpi_info = MPI.Info.Create()
    mpi_info.Set('romio_ds_read', 'disable')
    mpi_info.Set('romio_ds_write', 'disable')
    mpi_info.Set('striping_factor', str(striping_factor))
    mpi_info.Set('striping_unit', str(striping_unit)) 

    return mpi_info
