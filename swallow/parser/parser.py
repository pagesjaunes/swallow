from multiprocessing import TimeoutError, current_process
import sys
import logging
from swallow.logger_mp import get_logger_mp


def get_and_parse(p_inqueue, p_outqueue, p_process, p_counters, p_log_queue, p_log_level, p_formatter, **kwargs):
    """
        Gets doc from an input queue, applies transformation according to p_process function,
        then pushes the so produced new doc into an output queue

        p_process must take a "doc" as a first parameter

        @param p_inqueue    In queue containing docs to process
        @param p_outqueue   Out queue where processed docs are pushed
        @param p_process    function taking a doc as an input and returning a list of docs as a result
        @param p_nb_items_processed    Number of processed items
    """

    logger = get_logger_mp(__name__, p_log_queue, p_log_level, p_formatter)
    current = current_process()

    while True:
        try:
            logger.debug("(%s) Size of queues. in : %i / ou : %i", current.name, p_inqueue.qsize(), p_outqueue.qsize())

            try:
                in_doc = p_inqueue.get(False)
            except Exception:
                logger.debug("Nothing to get from the input queue")
            else:
                # Manage poison pill
                if in_doc is None:
                    logger.debug("(%s) => Parser has received 'poison pill' and is now ending ...", current.name)
                    p_inqueue.task_done()
                    break

                # Call the proc with the arg list (keeping the * means : unwrap the list when calling the function)
                out_doc = p_process(in_doc, **kwargs)

                for doc in out_doc:
                    p_outqueue.put(doc)

                p_inqueue.task_done()

                with p_counters['nb_items_processed'].get_lock():
                    p_counters['nb_items_processed'].value += 1
                    if p_counters['nb_items_processed'].value % p_counters['log_every'] == 0:
                        logger.info("Process in progress : {0} items processed".format(p_counters['nb_items_processed'].value))

        except TimeoutError:
            logger.warn('Timeout exception while parsing with %s method', p_process)
            with p_counters['nb_items_error'].get_lock():
                p_counters['nb_items_error'].value += 1
        except KeyboardInterrupt:
            logger.info("user interruption")
            sys.exit(0)
