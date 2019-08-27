import ctypes
import os
import re

# so we can sync from any method w/o passing stuff around
sysname = os.uname()[0]
if sysname.lower() != 'linux':
    def sync():
        return

else:
    libc = ctypes.CDLL("libc.so.6")

    def sync():
        libc.sync()
        return

def ratio_coefficient(ratio='1:1'):
    ratio_re = re.compile('([0-9]+):([0-9]+)')

    # check to see if the ratio is a valid N:M
    if not re.match(ratio_re, ratio):
        logging.error('{} is an invalid ratio, please use Writers:Readers (integers) format.'.format(ratio))
        return False

    left, right = re.match(ratio_re, ratio).groups()
    left = int(left)
    right = int(right)

    if right > left:
        split_ratio = (right / left) + 1
        split_left = split_ratio
        split_right = 1
    else:
        split_ratio = (left / right) + 1
        split_right = 1
        split_left = split_ratio

    return split_left, split_right
