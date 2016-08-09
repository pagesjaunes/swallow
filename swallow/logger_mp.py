import multiprocessing
from logging.handlers import QueueHandler
import logging


def get_logger_mp(p_name, p_log_queue, p_log_level, p_formatter):
    '''
        Init a logger for threads launched in multiprocessing mode
    '''
    qh = QueueHandler(p_log_queue)
    logger = logging.getLogger("mp." + p_name)
    logger.setLevel(p_log_level)
    logger.addHandler(qh)

    return logger
