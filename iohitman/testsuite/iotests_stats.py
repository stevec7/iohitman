import itertools
import sys
from mpi4py import MPI

from iohitman import stats

def gather_all_stats(w_comm=None, testcomm_name=None, testcomm_size=None, tags=None, stats_tuple=None):
    # stats_tuple needs to be the following format:
    # (testcomm_name, tags, testcomm_size, testname, test_runtime, bytes_rw,
    #   total_ios, total_buf_create_t, io_and_buf_runtime
    #   io_runtime)
    #
    # from there we'll use that info to print out the necessary stats
    #
    # this is messy, but I didnt want to pollute the other class methods

    combined_stats = [testcomm_name, testcomm_size, tags]  + list(itertools.chain(stats_tuple))

    all_results = w_comm.gather(combined_stats, root=0)

    if w_comm.rank == 0:

        data = sorted(all_results, key=lambda x: x[2])
        run_stats = {}
        for k, g in itertools.groupby(data, key=lambda x: x[2]):
            run_stats[k] = list((x[3:] for x in g))

        for k in run_stats:
            summed_results = stats.sum_results(run_stats[k])
            stats.show_test_results(summed_results, len(run_stats[k]), k)

        #for item in all_results:

    w_comm.Barrier()

    return
