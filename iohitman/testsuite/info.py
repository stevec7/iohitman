import logging

def show_test_info(kwargs):
    for k, v in kwargs.iteritems():
        logging.info("TEST_PARAMS: {}: {}".format(k, v))
    return
