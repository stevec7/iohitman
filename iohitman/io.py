import logging
import os
import sys
from mpi4py import MPI

from iohitman.bufutil import create_buffer, create_empty_buffer, randomize_buffer, create_boundaries
from iohitman.hints import set_mpi_file_info

def write(communicator=None, worklist=None, filename=None, buffersize=None, striping_factor=None):
    """
    write data to a file based on the test options and communicator

    args:
    communicator: the MPI communicator
    worklist: a numpy array of [start_offset, nbytes] combos, generated via the brm or brm_p methods
    filename: self explanatory

    returns: various stats
    """
    comm = communicator
    rand_key = comm.rank
    mpistatus = MPI.Status()

    if striping_factor is None:
        striping_factor = -1

    file_info = set_mpi_file_info(striping_factor=striping_factor)
    mode = MPI.MODE_WRONLY|MPI.MODE_CREATE
    if not os.path.isfile(filename):
        fd = MPI.File.Open(comm, filename, amode=mode, info=file_info)
    else:
        fd = MPI.File.Open(comm, filename, amode=mode)

    # figure out how to split our workload into even sized chunks of memory buffers
    bytes_written = 0
    boundaries = create_boundaries(worklist, buffersize)
    w_buf = create_buffer(buffersize, rand_key)
    total_buf_create_t = 0
    io_start_t = MPI.Wtime()
    io_sizes = worklist.T[1]    # transpose worklist and only take the iosizes

    try:
        for loop in range(0, len(boundaries)):
            start_w = boundaries[loop][0]
            end_w = boundaries[loop][1]

            bufpos = 0
            for i in worklist[start_w:end_w]:
                fd.Write_at(i[0], w_buf[bufpos:bufpos+i[1]], status=mpistatus)
                bufpos += i[1]
                bytes_written += mpistatus.count
    except MPI.Exception as e:
        logging.error("rank: {}, error: {}, message: {}, start: {}, nbytes: {}, bufpos: {}".format(
            comm.rank, e.error_string, e.message, i[0], i[1] - i[0], bufpos))
        comm.Abort()


    io_elapsed_t = MPI.Wtime() - io_start_t
    fd.Close()
    total_ios = len(worklist)

    return bytes_written, total_ios, total_buf_create_t, io_elapsed_t, io_elapsed_t - total_buf_create_t

def read(communicator=None, worklist=None, filename=None, buffersize=None, testname=None, striping_factor=None):
    """
    read data from a file based on test ops and the communicator

    args:
    communicator: the MPI communicator
    worklist: a numpy array of [start_offset, nbytes] combos, generated via the brm or brm_p methods
    filename: self explanatory

    returns: various stats
    """
    comm = communicator
    rand_key = comm.rank
    mpistatus = MPI.Status()

    if comm.size > 1:
        mode = MPI.MODE_RDONLY
    else:
        mode = MPI.MODE_RDONLY|MPI.MODE_UNIQUE_OPEN

    fd = MPI.File.Open(comm, filename, amode=mode)

    # figure out how to split our workload into even sized chunks of memory buffers
    bytes_read = 0
    boundaries = create_boundaries(worklist, buffersize)
    total_buf_create_t = 0
    r_buf = create_empty_buffer(buffersize)
    io_start_t = MPI.Wtime()
    io_sizes = worklist.T[1]    # transpose worklist and only take the iosizes

    try:
        for loop in range(0, len(boundaries)):
            start_w = boundaries[loop][0]
            end_w = boundaries[loop][1]

            bufpos = 0
            for i in worklist[start_w:end_w]:
                fd.Read_at(i[0], r_buf[bufpos:bufpos+i[1]], status=mpistatus)
                bufpos += i[1]
                bytes_read += mpistatus.count
    except MPI.Exception as e:
        logging.error("rank: {}, error: {}, message: {}, start: {}, nbytes: {}, bufpos: {}".format(
            comm.rank, e.error_string, e.message, i[0], i[1] - i[0], bufpos))
        comm.Abort()


    io_elapsed_t = MPI.Wtime() - io_start_t
    fd.Close()
    total_ios = len(worklist)

    return bytes_read, total_ios, total_buf_create_t, io_elapsed_t, io_elapsed_t - total_buf_create_t
