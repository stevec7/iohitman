import numpy as np

def create_buffer(buffersize, key):
    # this seems the fastest, and doesnt "suffer" from any zeroes compression
    #   these buffers will be too large to fit in L1/2/3 cache too, not that it would be a
    #   bad thing if they did, since all we want is fast buffer creation
    return bytearray(np.arange(0, buffersize / np.dtype('int').itemsize) + key)

def create_empty_buffer(buffersize):
    return bytearray(buffersize)

def randomize_buffer(buf):
    return bytearray(np.random.permutation(np.frombuffer(buf, dtype='uint64')))

def create_boundaries(worklist, buffersize):
    # this is used to split the worklist into similarly sized buffers
    #   the idea here is to create "boundaries" that can be used to easily determine
    #   at which point we need to allocate another buffer and continue, versus a mess
    #   of if statements during the I/O loops
    old_start = 0
    current_bufsize = 0
    boundaries = []

    for e, data in enumerate(worklist):
        num_bytes = data[1]
        current_bufsize += num_bytes

        if current_bufsize >= buffersize:
            boundaries.append((old_start, e))
            old_start = e
            current_bufsize = 0

    # need to pick up the last one, since it wont trip the if statement
    boundaries.append((old_start, e))

    return boundaries
