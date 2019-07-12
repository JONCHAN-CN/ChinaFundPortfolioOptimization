from functools import wraps
from time import time

from .logger import *


def try_exception(function):
    """
    A decorator that wraps the passed in function and logs exceptions should one occur
    https://www.blog.pythonlibrary.org/2016/06/09/python-how-to-create-an-exception-logging-decorator/
    """
    logger = init_logger('./log/exception.log', 'a', 'exception')

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except:
            # log the exception
            err = "There was an exception in  "
            err += function.__name__
            logger.exception(err)

            # # re-raise the exception
            # raise

    return wrapper


def time_elapsed(function):
    """
    A decorator that time passed in function
    https://codereview.stackexchange.com/questions/169870/decorator-to-measure-execution-time-of-a-function
    """

    @wraps(function)
    def wrapper(*args, **kw):
        ts = time()
        result = function(*args, **kw)
        te = time()
        # print('func:%r args:[%r, %r] took: %2.4f sec' % (function.__name__, args, kw, te-ts))
        print('func:%r took: %2.4f sec\n' % (function.__name__, te - ts))
        return result

    return wrapper
