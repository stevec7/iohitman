import logging
import numpy as np
from mpi4py import MPI

def create_buffer_list(iosize_range=(1048576, 1048576), num_ios=1048576):
    return np.array(np.random.random_integers(iosize_range[0], iosize_range[1], (num_ios,)), dtype='i')

# more or less, we'll do a lot of domain decomposition...
def decompose(comm, iosize_range=(1048576, 1048576), num_ios=1048576, interleaved=False, random=False, scatter_comm=None):
    '''
    comm should be the communicator you would like to generate the range off of.
    most other options are self explanatory

    however, scatter_comm could be a different communicator that you'd like to use to scatter the ranges between,
    if for instance you were running a mixed workload, with mixed sizes. this keeps the mixed size write ranges from
    being passed to ranks that need to do reads (also a different read range size).

    will only be used in when doing _decompose_random or _decompose_interleaved
    '''
    # Note: this works exactly the same as the serial versions in the byte_range_map module, but much faster.
    #    a surprise to be sure, but a welcome one
    rank = comm.Get_rank()
    commsize = comm.Get_size()
    local_buflist = create_buffer_list(iosize_range=iosize_range, num_ios=num_ios)

    local_cumsum = local_buflist.cumsum()
    local_blmax = local_cumsum.max()

    # TODO: use MPI Exclusive Scan
    if rank == 0:
        blmax = np.empty(commsize, dtype='int')
    else:
        blmax = None

    rc = comm.Gather([local_blmax, MPI.INT], [blmax, MPI.INT])

    # now we want each rank to add the previous ranks max to their own local cumsum
    if rank == 0:
        all_blmax = blmax
    else:
        all_blmax = np.empty(commsize, dtype='int')
    rc = comm.Bcast([all_blmax, MPI.INT])

    if rank == 0:
        add_bytes = 0
        adjusted_bl = np.insert(local_buflist, 0, 0)
        adjusted_bl = adjusted_bl.cumsum()
        adjusted_bl = np.delete(adjusted_bl, -1)
    else:
        add_bytes = all_blmax[:rank].sum()
        adjusted_bl = np.insert(local_buflist, 0, 0)
        adjusted_bl = adjusted_bl.cumsum() + add_bytes
        adjusted_bl = np.delete(adjusted_bl, -1)

    # join the adjusted buffer list with the original buffer list to create many [start_offset, num_bytes] entries
    br_map = np.column_stack((adjusted_bl, local_buflist))
    local_buflist = None
    adjusted_bl = None

    if scatter_comm is None:
        scatter_comm = comm

    if random: # takes 8 seconds for 100M
        my_range = _decompose_random(scatter_comm, br_map)
    elif interleaved: # takes 3 seconds for 100M
        my_range = _decompose_interleaved(scatter_comm, br_map)
    else:
        # this is segmented I/O (one sequential range within a file per rank). The requests could be
        #   unaligned if you pass in a io size range that isn't (1M, 1M), but other than that,
        #   they will be aligned and sequential
        my_range = br_map

    return my_range

def _decompose_interleaved(comm, br_map):
    # this will grab every comm.size-th element from every other rank, and then use that
    for rank in range(0, comm.size):
        if comm.rank == rank:
            new_br_map = np.empty(br_map.shape, dtype='int')
        else:
            new_br_map = None
        send_data = br_map[rank::comm.size].copy()  # have to do a copy, as slicing isn't contiguous on its own
        rc = comm.Gather([send_data, MPI.INT], [new_br_map, MPI.INT], root=rank)    # key here is root=rank

        if comm.rank == rank:
            my_range = new_br_map.copy()
            new_br_map = None
    br_map = None
    comm.Barrier()
    return my_range

def _decompose_random(comm, br_map):
    # this is similar to the interleaved mode, but I shuffle the array for each rank
    #    before we Gather it, and then before we return as well. I think that's random enough...
    rc = np.random.shuffle(br_map)
    my_range = _decompose_interleaved(comm, br_map)
    rc = np.random.shuffle(my_range)
    comm.Barrier()
    return my_range

# keeping just so i have an easy to find parallel shuffle sample
#
#def _decompose_random_slow(comm, br_map):
#    # stolen from http://stackoverflow.com/questions/36266968/parallel-computing-shuffle
#    for step in range(1, comm.size+1):
#        if ((comm.rank + step) % 2) == 0:
#            if comm.rank < comm.size - 1:
#                br_map = exchange_shuffle(comm, br_map, comm.rank, comm.rank + 1)
#        elif comm.rank > 0:
#            br_map = exchange_shuffle(comm, br_map, comm.rank - 1, comm.rank)
#    return br_map
#
#def exchange_shuffle(comm, local_data, sendrank, recvrank):
#
#    rank = comm.Get_rank()
#    if rank == sendrank:
#        newdata = np.empty(local_data.shape, dtype='int')
#        rc = comm.Send(local_data, dest=recvrank)
#        rc = comm.Recv(newdata, source=recvrank)
#    else:
#        bothdata = np.empty((len(local_data) * 2, 2), dtype='int')
#        otherdata = np.empty(local_data.shape, dtype='int')
#        rc = comm.Recv(otherdata, source=sendrank)
#        bothdata = np.concatenate((local_data, otherdata))
#        rc = np.random.shuffle(bothdata)
#        rc = comm.Send(bothdata[:len(otherdata)], dest=sendrank)
#        newdata = bothdata[len(otherdata):]
#
#    return newdata
