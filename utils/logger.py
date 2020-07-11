import logging
import os
import sys
from datetime import datetime as dt


def init_logger(filename=None, filemode='a', logger_name='main'):
    if filename is None:
        filename = './log/%s_%s.log' % (os.path.basename(sys.argv[0]).strip('.py'), dt.now().strftime('%Y-%m-%d'))
    if not os.path.exists(os.path.dirname(filename)):
        os.mkdir(os.path.dirname(filename))
    logging.basicConfig(level=logging.INFO,
                        filename=filename,
                        filemode=filemode,
                        datefmt='%Y/%m/%d %H:%M:%S',
                        format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                        )
    logger = logging.getLogger(logger_name)
    logger.addHandler(logging.StreamHandler())
    if not ('exception' in filename):
        logger.info('logging in %s' % filename)
    return logger