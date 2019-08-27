import math

def round_up(n, multiple):
    """
    can be used to ensure that you have next multiple if you need an int, but have a float

    n = number to check
    multiple = number you'd like to round up to

    EX:
    num_ranks = 64
    num_ios = 26252

    this is not an even number if you divide

    returns the next multiple (upper)
    """
    return math.ceil(n / (multiple + 0.0)) * multiple
