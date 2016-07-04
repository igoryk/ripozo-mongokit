import cProfile
import pstats

import unittest2


def profileit(func):
    """
    Decorator straight up stolen from stackoverflow
    """
    def wrapper(*args, **kwargs):
        datafn = func.__name__ + ".profile" # Name the data file sensibly
        prof = cProfile.Profile()
        prof.enable()
        retval = prof.runcall(func, *args, **kwargs)
        prof.disable()
        stats = pstats.Stats(prof)
        stats.sort_stats('tottime').print_stats(20)
        print()
        print()
        stats.sort_stats('cumtime').print_stats(20)
        return retval

    return wrapper


class TestProfile(unittest2.TestCase):
    pass