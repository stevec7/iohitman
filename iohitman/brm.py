import numpy as np

def add_offsets(buffer_list):
    # first insert a zero at the beginning (start at file offset zero)
    # next join the cumulative sum of the offsets (0, 1023, 2047) to the offset list (1024, 1024, 1024) that is shifted one element
    # then remove the last element, which would be (${somelargenumber}, 0)
    #
    # numpy is great, and this was much faster than for loops, and should scale much higher due to numpy vector operations
    buffer_list = np.insert(buffer_list, 0, 0)
    br_map = np.column_stack((buffer_list.cumsum(), np.roll(buffer_list, -1)))
    return np.delete(br_map, -1, axis=0)

def create_buffer_list(iosize_range=(1048576, 1048576), num_ios=1048576):
    return np.array(np.random.random_integers(iosize_range[0], iosize_range[1], (num_ios,)), dtype='i')

def create_map(comm, iosize_range=(1048576, 1048576), num_ios=1048576, interleaved=False, random=False, scatter_comm=None):
    buffer_list = create_buffer_list(iosize_range=iosize_range, num_ios=num_ios)
    br_map = add_offsets(buffer_list)
    worklist = np.empty((comm.size, len(buffer_list)), dtype=('int', 'int'))

    if random:
        n = np.random.shuffle(br_map)   # done in place, n = None
        return br_map
    elif interleaved:
        #skip = int((sum(iosize_range) / 2) * num_ios / 32)    # i picked 32 because reasons
        skip = num_ios / 32 # i picked 32 because reasons
        worklist = np.concatenate(np.array([br_map[x::skip] for x in range(skip)]))
    else:
        worklist = br_map
    return worklist

    # the method below is more complicated because I also have to partition the arrays. it doesnt make
    #   sense to do that if we're only going to scatter them to the ranks (which partitions them anyway)
    #
    #if random:
    #    for rank in range(0, num_workers):
    #        # what is essentially happening here is that we create
    #        #   an array of False that is the same shape as the br_map array
    #        #
    #        #   next we choose a bunch of items out at random, and set their values to true
    #        #
    #        #   then we broadcast the True values from indices on top of br_map
    #        #
    #        #   lastly we remove those chosen entries from br_map
    #        indices = np.full(br_map.shape[0], False, dtype=bool)
    #        chosen_indices = np.random.choice(indices.shape[0], ios_per_rank, replace=False)
    #        indices[chosen_indices] = True
    #        worklist[rank] = br_map[chosen_indices]
    #        br_map = br_map[~indices]
    #elif interleaved:
    #    worklist = np.array([br_map[x::num_workers] for x in range(0, num_workers)])
    #    # take every nth (num_workers) item
    #    #for rank in range(0, num_workers):
    #    #    worklist[rank] = br_map[rank::num_workers]
    #else:
    #    # segmented random
    #    worklist = np.split(br_map, num_workers)
    #    #worklist = br_map.reshape(-1, num_workers)
    ##print len(worklist), len(worklist[0]), worklist[0]
    #return worklist
