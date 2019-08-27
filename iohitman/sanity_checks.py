import logging

def check_options(options):
    bad = False
    if options.buffersize % options.transfersize != 0:
        logging.info("Transfer size is not a multiple of buffer size.")
        bad = True
    if options.filesize % options.buffersize != 0:
        logging.info("File size is not a multiple of buffer size.")
        bad = True
    if options.filesize % 8 != 0:
        logging.info("File size is not divisible by 8 bytes (byte size of each random int)")
        bad = True

    if bad == True:
        return False

    return True

def check_options_randomio(options):
    bad = False
    if options.buffersize % options.transfersize != 0:
        logging.info("Transfer size is not a multiple of buffer size.")
        bad = True
    if options.filesize % options.buffersize != 0:
        logging.info("File size is not a multiple of buffer size.")
        bad = True
    if options.filesize % 8 != 0:
        logging.info("File size is not divisible by 8 bytes (byte size of each random int)")
        bad = True

    if bad == True:
        return False

    return True
