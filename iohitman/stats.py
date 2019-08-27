import logging

def show_test_results(summary, num_ranks, tag='NULL'):
    total_gb = summary[0] / 1024 / 1024 / 1024
    iops = (summary[1] / summary[4]) * num_ranks
    test_time = summary[4] / num_ranks
    io_tm = summary[4] / num_ranks
    mbs = (total_gb * 1024) / io_tm
    avg_io_tm = "{:5f}".format(io_tm / summary[1])
    logging.info("RESULTS: test_name: {}, ranks: {}, total_gb_rw: {}, mbs: {}, iops: {} test_time: {}, avg_io_tm: {}".format(
        tag, num_ranks, total_gb, mbs, iops, test_time, avg_io_tm))

    return

def sum_results(results):
    total_bytes = sum(x[0] for x in results)
    total_ios = sum(x[1] for x in results)
    total_buf_op_time = sum(x[2] for x in results)
    total_test_time = sum(x[3] for x in results)
    total_io_time = sum(x[4] for x in results)
    return total_bytes, total_ios, total_buf_op_time, total_test_time, total_io_time
