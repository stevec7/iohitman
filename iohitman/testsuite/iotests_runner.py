import json
import logging
import numpy as np
import os
import re
import sys
import time

from collections import namedtuple
from mpi4py import MPI
from pathlib2 import Path

# custom modules
from iohitman import brm, brm_p
from iohitman import common
from iohitman import communicators
from iohitman import mathutil
from iohitman import io
from iohitman import stats
from iohitman.testsuite import iotests_defs as test_defs
from iohitman.testsuite import iotests_stats
from iohitman.testsuite import info

class IOHitmanRunner(object):
    def __init__(self, testname, options, w_comm, dry_run=False):
        self._version = 3
        self.testname = testname
        self.w_comm = w_comm.Dup()
        self.dry_run = dry_run
        self.nodename = MPI.Get_processor_name()

        # need some test names
        if re.match('^[0-9]', options.testname):
            self.testname = test_defs.valid_tests[int(options.testname)]

        self.w_comm.Barrier()
        self.testname_attrs = self.testname.split('_')
        self.io_pattern = self.testname_attrs[2]
        self.io_name = self.testname_attrs[1]
        self.io_type = self.testname_attrs[0]
        self.keepfiles = True
        self.striping_factor = -1

        # turn all command line args into class attributes
        l_options = vars(options)
        self.options = namedtuple('options', l_options.keys())
        for k, v in l_options.iteritems():
            rc = setattr(self.options, k, v)
            rc = setattr(self, k, v)

        # use the other passed in options to determine read/write patterns
        self.patterns = {'readers': self.readpattern, 'writers': self.writepattern}

        # nowhere else to put it, sorry
        self.options.testname = test_defs.valid_tests[int(options.testname)]
        self.testname = test_defs.valid_tests[int(testname)]

        if self.testname.split('_')[1] == 'fileperprocess' or self.testname.split('_')[1] == 'filepernode':
            self.striping_factor = 1

        # create a communicator per node too
        self.nodecomm = communicators.create_per_node(self.w_comm)

        return

    @classmethod
    def create(cls, testname, options, w_comm, dry_run=False):
        return cls(testname, options, w_comm)


    def run(self):
        # show test info
        if self.w_comm.rank == 0:
            rc = self.show_test_info()
        self.w_comm.Barrier()

        #logging.debug('rank: {}, commname: {}, io_type: {}'.format(self.w_comm.rank, self.testcomm_name, self.io_type))

        if self.dry_run is True:
            logging.info("Dry run mode enabled, stopping here...")
            return

        if self.io_type == 'writeread':
            w_results = self.run_write_test()

            # if options.reordertasks is enabled, on read, ranks will read different data they wrote...
            #
            # rank 0 will randomize the rank list, and then we'll send/receive worklists between
            #   partners: 
            #
            #   * ranklist[myrank] = rank to send worklist to
            #   * ranklist index where value is my world rank, = rank to receive from
            #
            #   I can tell you that the np.where pieces wont scale but I'm not worried for now
            if self.reordertasks:
                commsize = self.w_comm.Get_size()
                if self.w_comm == 0:
                    num_ranks = commsize
                    ranks = np.arange(0, num_ranks)
                    ranks = np.random.shuffle(ranks)
                else:
                    ranks = np.empty(commsize, dtype='int')
                self.w_comm.Bcast([ranks, MPI.INT])

                sendto = ranks[self.w_comm.rank]
                sendsize = len(self.worklist)
                receivefrom = int(np.where(ranks == self.w_comm.rank)[0])
                rcvsize = np.empty(1, dtype='int')

                if self.w_comm.rank % 2 == 0:
                    # recv first
                    self.w_comm.Recv(rcvsize, source=receivefrom)
                    new_worklist = np.empty(rcvsize, dtype='int')
                    self.w_comm.Recv(new_worklist, source=receivefrom)

                    # now send them data
                    self.w_comm.Send(sendsize, dest=sendto)
                    self.w_comm.Send(self.worklist, dest=sendto)

                    self.worklist = new_worklist
                    new_worklist = None

                else:
                    # send first
                    self.w_comm.Send(sendsize, dest=sendto)
                    self.w_comm.Send(self.worklist, dest=sendto)

                    # receive
                    self.w_comm.Recv(rcvsize, source=receivefrom)
                    self.worklist = np.empty(rcvsize, dtype='int')
                    self.w_comm.Recv(self.worklist, source=receivefrom)

               
            r_results = self.run_read_test()
            #rc = iotests_stats.gather_all_stats(self.w_comm, w_results)
            rc = iotests_stats.gather_all_stats(self.w_comm, self.testcomm_name,
                self.testcomm.size, 'RESULTS {}_WRITE'.format(self.testname), w_results)
            #rc = iotests_stats.gather_all_stats(self.w_comm, r_results)
            rc = iotests_stats.gather_all_stats(self.w_comm, self.testcomm_name,
                self.testcomm.size, 'RESULTS {}_READ'.format(self.testname), r_results)
        elif self.io_type == 'mixed':
            if self.testcomm_name == 'readers':
                tg = 'READ'
                results = self.run_read_test()
            else:
                tg = 'WRITE'
                results = self.run_write_test()

            rc = iotests_stats.gather_all_stats(self.w_comm, self.testcomm_name,
                self.testcomm.size, 'RESULTS {}_{}'.format(self.testname, tg), results)
        elif self.io_type == 'read':
            results = self.run_read_test()
            rc = iotests_stats.gather_all_stats(self.w_comm, self.testcomm_name,
                self.testcomm.size, 'RESULTS {}_READ'.format(self.testname), results)
        else:
            results = self.run_write_test()
            rc = iotests_stats.gather_all_stats(self.w_comm, self.testcomm_name,
                self.testcomm.size, 'RESULTS {}_WRITE'.format(self.testname), results)

        if not self.options.keepfiles and self.testcomm.rank == 0:
            # self.testcomm.rank will equal 0 for any file per process communicators too,
            #   since MPI_COMM.SELF only has one member
            logging.debug("rank: {}, deleting file: {}".format(self.w_comm.rank, self.testfilename))
            rc = MPI.File.Delete(self.testfilename)

        return

    def setup_communicator(self):
        # more complicated, ugly logic
        if self.io_type == 'mixed':
            if self.io_name == 'fileperprocess':
                #self.testcomm = communicators.create_ratio_fpp(self.w_comm, ratio=self.options.ratio)
                self.testcomm = MPI.COMM_SELF
            elif self.io_name == 'filepernode':
                self.testcomm = communicators.create_ratio(self.nodecomm, ratio=self.options.ratio)
            else:
                self.testcomm = communicators.create_ratio(self.w_comm, ratio=self.options.ratio)
            self.testcomm_name = self.testcomm.Get_name()

        elif self.io_type == 'read':
            if self.io_name == 'fileperprocess':
                self.testcomm = MPI.COMM_SELF
            elif self.io_name == 'filepernode':
                self.testcomm = self.nodecomm.Dup()
            else:
                self.testcomm = self.w_comm.Dup()
            self.testcomm_name = 'readers'

        elif self.io_type == 'write':
            if self.io_name == 'fileperprocess':
                self.testcomm = MPI.COMM_SELF
            elif self.io_name == 'filepernode':
                self.testcomm = self.nodecomm.Dup()
            else:
                self.testcomm = self.w_comm.Dup()
            self.testcomm_name = 'writers'
        else:
            if self.io_name == 'fileperprocess':
                self.testcomm = MPI.COMM_SELF
            elif self.io_name == 'filepernode':
                self.testcomm = self.nodecomm.Dup()
            else:
                self.testcomm = self.w_comm.Dup()
            self.testcomm_name = 'world'


            return

    def setup_test(self):
        # this is incredibly ugly, but short of redesigning this to use some short
        #   of bitmasking to configure all of the various test combinations, its
        #   the best I can do for now...
        #
        # we need to figure out what communicator to use, the number of IOs, etc
        rc = self.setup_communicator()
        if self.io_name == 'fileperprocess':
            self.testfilename = self.options.filename + ".{}".format(self.w_comm.rank)
        elif self.io_name == 'filepernode':
            self.testfilename = self.options.filename + ".{}".format(self.nodename)
        else:
            self.testfilename = self.options.filename

        # if using custom read/write ranges per read or write section, set the iosize ranges
        if self.testcomm.Get_name() == 'writers':
            ioszrange = self.options.writesize
        elif self.testcomm.Get_name() == 'readers':
            ioszrange = self.options.readsize
        else:
            ioszrange = self.options.iosize_range

        # dont need to check for segmented, as False/False for the two attrs causes that to happen
        rand = False
        inter = False
        self.mypattern = self.io_pattern
        if self.io_pattern == 'random':
            rand = True
            # this is done so the output isn't confusing later
            self.options.writepattern = self.io_pattern
            self.options.readpattern = self.io_pattern
        elif self.io_pattern == 'interleaved':
            inter = True
            # this is done so the output isn't confusing later
            self.options.writepattern = self.io_pattern
            self.options.readpattern = self.io_pattern
        # this will someday be improved time permitting, but for now its a hot mess, i hate it
        elif self.io_pattern == 'mixed':
            self.mypattern = self.patterns[self.testcomm.Get_name()]
            if self.mypattern == 'random':
                rand = True
            elif self.mypattern == 'interleaved':
                inter = True

        brm_comm = self.testcomm.Dup() # need to overwrite this for mixed workloads
        scatter_comm = None
        if self.io_type == 'mixed':
            brm_comm = self.w_comm.Dup()
            scatter_comm = self.testcomm.Dup()
            self.num_ios = (self.blocksize / (sum(x for x in ioszrange) / 2))
        else:
            self.num_ios = self.blocksize / (sum(x for x in ioszrange) / 2)

        # if the number of ios isn't a multiple of the communicator size, fix that
        #
        # if it already is, nothing will change
        self.num_ios = int(mathutil.round_up(self.num_ios, self.testcomm.size))

        worklist_create_opts = {
                'iosize_range': ioszrange,
                'num_ios': self.num_ios,
                'interleaved': inter,
                'random': rand,
                'scatter_comm': scatter_comm,
        }
        if self.testcomm.size == 1:
            self.worklist = brm.create_map(brm_comm, **worklist_create_opts)
        else:
            self.worklist = brm_p.decompose(brm_comm, **worklist_create_opts)

        logging.debug("rank: {}, FINISHED_CREATING_WORKLIST, pattern: {}/{}, iosize_range: {}, num_ios: {}, len(worklist): {},\
                worklist_sample: {}".format(self.w_comm.rank, self.io_pattern, self.mypattern, ioszrange, self.num_ios, len(self.worklist), self.worklist[:3]))

        # dumps a lot of debugging data
        try:
            if int(os.environ['IOHITMAN_DUMP_DEBUG_DATA']) == 1:
                self._dump_debug_data()
        except ValueError:
            logging.debug('IOHITMAN_DUMP_DEBUG_DATA is not an int, ignoring')
            pass

        return

    def run_read_test(self):
        file_checks = Path(self.testfilename)
        if not file_checks.is_file() and not self.testfilename.startswith('ime:'):
            logging.error("rank: {}, file: {}, does not exist, aborting...".format(self.w_comm.rank, self.testfilename))
            self.w_comm.Abort()

        results = io.read(communicator=self.testcomm, worklist=self.worklist,
            filename=self.testfilename,
            buffersize=self.options.buffersize, striping_factor=self.striping_factor)
        logging.debug("rank: {}, results: {}".format(self.w_comm.rank, results))
        return results

    def run_write_test(self):
        results = io.write(communicator=self.testcomm, worklist=self.worklist,
            filename=self.testfilename,
            buffersize=self.options.buffersize, striping_factor=self.striping_factor)
        logging.debug("rank: {}, results: {}".format(self.w_comm.rank, results))
        return results

    def show_test_info(self):
        kwargs = {
                'iohitman_version': self._version,
                'test_name': self.testname,
                'cmd_line': ' '.join(sys.argv[1:]),
                'num_ranks': self.w_comm.Get_size(),
        }
        for field in self.options._fields:
            kwargs[field] = getattr(self.options, field)

        rc = info.show_test_info(kwargs)

        return

    def _dump_debug_data(self):
        # setup metadata
        dump_data = {}
        metadata = {}
        metadata['world_rank'] = self.w_comm.rank
        metadata['testcomm_rank'] = self.testcomm.rank
        metadata['filename'] = self.testfilename
        metadata['communicator_name'] = self.testcomm.Get_name()
        metadata['testname'] = self.testname
        metadata['time'] = time.time()
        metadata['num_ios'] = len(self.worklist)
        metadata['tags'] = self.options.tags
        metadata['cmd_line'] = ' '.join(sys.argv[1:])
        dump_data['metadata'] = metadata
        dump_data['data'] = self.worklist.tolist()

        if os.path.dirname(os.path.abspath(os.environ['IOHITMAN_DUMP_DEBUG_DATA_DIR'])):
            dumpfile = '{}{}iohitmandebug.rank{}.json'.format(
                    os.environ['IOHITMAN_DUMP_DEBUG_DATA_DIR'], os.path.sep, self.w_comm.rank)
            logging.debug('IOHITMAN_DUMP_DEBUG_DATA set, dumping debug data out to: {}'.format(dumpfile))

            with open(dumpfile, 'wb') as f:
                json.dump(dump_data, f)

        else:
            logging.debug('IO_DUMP_DEBUG_DATA set, but parent directory does not exist, not dumping.')

        return


class IOHitmanRunnerException(Exception):
    def __init__(self, message, errors, die=False):
        self.message = message
        self.errors = errors

        logging.error("IOHitmanRunnerException: Msg: {}, Errors: {}".format(message, errors))
        sys.exit(1)
