#!/usr/bin/env python
#
import itertools
import logging
import numpy as np
import operator
import os
import random
import socket
import sys
from mpi4py import MPI
from optparse import OptionParser
from argparse import ArgumentParser

from iohitman.testsuite import iotests_runner as ts
from iohitman.testsuite import iotests_defs as test_defs


def main(options, args):
    comm = MPI.COMM_WORLD
    w_comm = comm.Dup()
    w_size = w_comm.Get_size()
    w_rank = w_comm.Get_rank()
    w_group = w_comm.Get_group()

    if options.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    testname = options.testname

    # take care of some defaults that might be left out...
    if options.iosize_range is None and options.transfersize:
        options.iosize_range = [int(options.transfersize), int(options.transfersize)]
    elif options.iosize_range is None and not options.transfersize:
        options.iosize_range = [int(1024 * 1024), int(1024 * 1024)]
    else:
        options.iosize_range = [int(x) for x in options.iosize_range.split(',')]

    if options.readsize is None:
        options.readsize = options.iosize_range
        #options.readsize = [int(x) for x in options.iosize_range.split(',')]
    elif options.readsize and len(options.readsize.split(',')) == 1:
        options.readsize = [int(options.readsize), int(options.readsize)]
    else:
        options.readsize = [int(x) for x in options.readsize.split(',')]

    if options.writesize is None:
        #options.writesize = [int(x) for x in options.iosize_range.split(',')]
        options.writesize = options.iosize_range
    elif options.writesize and len(options.writesize.split(',')) == 1:
        options.writesize = [int(options.writesize), int(options.writesize)]
    else:
        options.writesize = [int(x) for x in options.writesize.split(',')]

    if int(testname) not in test_defs.valid_tests.keys() and testname not in test_defs.valid_tests_r.keys():
        if w_comm.rank == 0:
            logging.error('--test-name {} is not valid. Please choose one of the following (name or number):\n'.format(
            options.testname))
            test_defs.print_valid_tests()
            w_comm.Abort()
    #if w_comm.rank == 0:
    #    print options.iosize_range, options.readsize, options.writesize

    test = ts.IOHitmanRunner.create(options.testname, options, w_comm, dry_run=options.dry_run)
    test.setup_test()
    test.run()


    #if not options.dry_run:
    #else:
    #    if w_comm.rank == 0:
    #        test.show_test_info()

    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-b', '--buffer-size', dest='buffersize', type='int', metavar='NUM_BYTES', help='buffer size in bytes for each rank. \
                    DO NOT allocate more than client memory, remember, total amount mem used per node is: ${buffersize} * mpi_ranks_per_node...128MB is a good choice. ')
    parser.add_option('-i', '--iosz-range', dest='iosize_range', default=None, type='str', metavar='LOW_BYTES,HIGH_BYES', help='if you pass one value, \
                    that is the io size. if you pass two, io sizes are generated between that range. Ex: -i 1024 == 1KB, -i 1024,2048 == 1KB-2KB. Overwrites -t value')
    parser.add_option('-k', '--keep-files', dest='keepfiles', default=False, action='store_true', metavar='ENABLE_KEEP_FILES', help='keep files after run')
    parser.add_option('-l', '--show-test-names', dest='showtestnames', default=False, metavar='SHOW_TEST_NAMES', action='store_true', help='Show all valid test names')
    parser.add_option('-o', '--output-filename', dest='filename', metavar='FILENAME_PREFIX', help='path and filename prefix to write and read to. \
                    output file per node will be: ${filename}.${nodename}.dat. Make sure its on a fast filesystem')
    parser.add_option('-s', '--block-size', dest='blocksize', type='int', metavar='NUM_BYTES', help='number of bytes to write/read per MPI rank.')
    parser.add_option('-t', '--transfer-size', dest='transfersize', type='int', metavar='NUM_BYTES', help='io transfer size in bytes')
    parser.add_option('-v', '--verbose', dest='verbose', default=False, action='store_true', metavar='ENABLE_VERBOSE', help='enable verbose logging')
    #parser.add_option('--disable-barriers', dest='nobarriers', default=False, metavar='DISABLE_BARRIERS', action='store_true', help='Disable barriers between read and write tests')
    parser.add_option('--dry-run', dest='dry_run', default=False, action='store_true', metavar='DRY_RUN', help='show the test info and create the byte range worklists, but dont actually run the tests.')
    parser.add_option('--ratio', dest='ratio', default='1:1', metavar='WRITERS_PCT:READERS_PCT', help='If running a test that starts with \"mixed\",\
                    you can set the percentage of writes vs readers. Ignored on other tests')
    parser.add_option('--read-size', dest='readsize', default=None, type='str', metavar='LOW_BYTES,HIGH_BYES', help='read size in bytes. can take multiple args for ranges instead, similar to -i')
    parser.add_option('--reorder-tasks', dest='reordertasks', default=False, metavar='REORDER_TASKS', action='store_true', help='When doing both write and read tests (testname starts with writeread), on reads different byte ranges will be used to avoid caching')
    parser.add_option('--test-name', dest='testname', default='writeread_fileperprocess_segmented', help='REQUIRED. The type of I/O test to run. Pass --show-test-names to see a full list. You can pass the name or number')
    parser.add_option('--write-size', dest='writesize', default=None, type='str', metavar='LOW_BYTES,HIGH_BYES', help='write size in bytes. can take multiple args for ranges instead, similar to -i')
    parser.add_option('--tags', dest='tags', default=None, type='string', metavar='SOME,TAGS', help='any custom tags that one would like it see in the output')
    parser.add_option('--read-pattern', dest='readpattern', default='segmented', type='choice', choices=['random', 'segmented', 'interleaved'], metavar='PATTERN_TYPE', help='if you are doing a mixed workload, you can specify the type of I/O pattern for the readers, different from the writers. Choose between [random|segmented|interleaved]')
    parser.add_option('--write-pattern', dest='writepattern', default='segmented', type='choice', choices=['random', 'segmented', 'interleaved'], metavar='PATTERN_TYPE', help='if you are doing a mixed workload, you can specify the type of I/O pattern for the writers, different from the readers. Choose between [random|segmented|interleaved]')

    options, args = parser.parse_args()

    if options.showtestnames:
        test_defs.print_valid_tests()
        sys.exit(0)

    main(options, args)
