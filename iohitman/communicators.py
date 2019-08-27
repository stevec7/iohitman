import logging
import numpy as np
import operator
import re

from mpi4py import MPI

def create_per_node(w_comm):
    # find out each ranks hostname so that we can make communicators, just to be 100%
    #    I realize there is a better way to use MPI_Dims + mpirun options, but I'm going to keep it simple...
    #
    w_rank = w_comm.Get_rank()
    w_group = w_comm.Get_group()
    myhostname = MPI.Get_processor_name()
    hostmap_raw = w_comm.gather(myhostname, root=0)

    # create groups of nodes first
    if w_rank == 0:
        node_map = zip(range(len(hostmap_raw)), hostmap_raw)
        groups = {}
        for k, v in node_map:
            if not groups.get(v):
                groups[v] = []
                groups[v].append(k)
            else:
                groups[v].append(k)
    else:
        groups = None
    w_comm.Barrier()

    groups = w_comm.bcast(groups, root=0)
    mygroup_ranks = groups[myhostname]
    mygroup = w_group.Incl(mygroup_ranks)
    group_comm = w_comm.Create(mygroup)
    group_comm.Set_name(myhostname)

    return group_comm

def create_odd_even(w_comm):
    '''
    rank is either in the odd or even communicator
    '''
    color = w_comm.rank % 2
    if color == 0:
        key = w_comm.rank
    else:
        key = -w_comm.rank

    newcomm = w_comm.Split(color, key)
    newcomm.Set_name(str(color))

    return newcomm

def create_ratio(w_comm, ratio='1:1'):
    '''
    create two communicators, N->1
    '''

    ratio_re = re.compile('([0-9]+):([0-9]+)')

    # check to see if the ratio is a valid N:M
    if not re.match(ratio_re, ratio):
        logging.error('{} is an invalid ratio, please use Writers:Readers (integers) format.'.format(ratio))
        w_comm.Abort()

    left, right = re.match(ratio_re, ratio).groups()
    left = int(left)
    right = int(right)

    if right > left:
        split_ratio = (right / left) + 1
        favor_readers = True
    else:
        split_ratio = (left / right) + 1
        favor_readers = False

    w_size = w_comm.Get_size()
    w_rank = w_comm.Get_rank()
    w_group = w_comm.Get_group()
    ranks = np.arange(w_comm.Get_size())

    if favor_readers:
        writers = ranks[::split_ratio]
        readers = np.setdiff1d(ranks, writers)
    else:
        readers = ranks[::split_ratio]
        writers = np.setdiff1d(ranks, readers)

    if w_rank in readers:
        mygroup = w_group.Incl(readers)
        newcomm = w_comm.Create(mygroup)
        rc = newcomm.Set_name('readers')
    else:
        mygroup = w_group.Incl(writers)
        newcomm = w_comm.Create(mygroup)
        rc = newcomm.Set_name('writers')

    return newcomm

def create_ratio_fpp(w_comm, ratio='1:1'):
    '''
    only use this when doing mixed io, and file-per-process. it will create a
    communicator of COMM_SELF, but also set a comm name for either a reader or writer

    '''

    ratio_re = re.compile('([0-9]+):([0-9]+)')

    # check to see if the ratio is a valid N:M
    if not re.match(ratio_re, ratio):
        logging.error('{} is an invalid ratio, please use Writers:Readers (integers) format.'.format(ratio))
        w_comm.Abort()

    left, right = re.match(ratio_re, ratio).groups()
    left = int(left)
    right = int(right)

    if right > left:
        split_ratio = (right / left) + 1
        favor_readers = True
    else:
        split_ratio = (left / right) + 1
        favor_readers = False

    w_size = w_comm.Get_size()
    w_rank = w_comm.Get_rank()
    w_group = w_comm.Get_group()
    ranks = np.arange(w_comm.Get_size())

    if favor_readers:
        writers = ranks[::split_ratio]
        readers = np.setdiff1d(ranks, writers)
    else:
        readers = ranks[::split_ratio]
        writers = np.setdiff1d(ranks, readers)

    if w_rank in readers:
        newcomm = MPI.COMM_SELF
        rc = newcomm.Set_name('readers')
    else:
        newcomm = MPI.COMM_SELF
        rc = newcomm.Set_name('writers')

    return newcomm
