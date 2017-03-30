from multiprocessing import TimeoutError
from swallow.logger_mp import get_logger_mp
import time
try:
    from Queue import Empty
except ImportError:
    from queue import Empty


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

    start = time.time()
    start_idle = None
    # Main loop max retry
    main_loop_max_retry = 5
    main_loop_retry = 0
    queue_get_timeout = 60

    while True:
        try:
            try:
                in_doc = p_inqueue.get(block=True, timeout=queue_get_timeout)
            except Empty:
                # Idle starts with the first exception (queue empty)
                logger.debug("No doc in queue in the last {}s".format(queue_get_timeout))
                if not start_idle:
                    start_idle = time.time()
            else:
                if start_idle:
                    elapsed_idle = time.time() - start_idle
                else:
                    elapsed_idle = 0

                # Manage poison pill
                if in_doc is None:
                    p_inqueue.task_done()
                    break

                # Call the proc with the arg list (keeping the * means : unwrap the list when calling the function)
                start_p_process = time.time()

                out_doc = p_process(in_doc, **kwargs)

                elapsed_p_process = time.time() - start_p_process

                for doc in out_doc:
                    p_outqueue.put(doc)

                p_inqueue.task_done()

                with p_counters['nb_items_processed'].get_lock():
                    p_counters['nb_items_processed'].value += 1
                    now = time.time()
                    elapsed = now - start

                    p_counters['whole_process_time'].value += elapsed
                    p_counters['real_process_time'].value += elapsed_p_process
                    p_counters['idle_process_time'].value += elapsed_idle

                    nb_items = p_counters['nb_items_processed'].value
                    if p_counters['nb_items_processed'].value % p_counters['log_every'] == 0:
                        logger.info("Process : {0} items".format(nb_items))
                        logger.debug("   -> Avg process time   : {0}ms".format(1000 * p_counters['whole_process_time'].value / nb_items))
                        logger.debug("   -> Avg real time      : {0}ms".format(1000 * p_counters['real_process_time'].value / nb_items))
                        logger.debug("   -> Avg idle time      : {0}ms".format(1000 * p_counters['idle_process_time'].value / nb_items))
                        logger.debug("State of queues :")
                        logger.debug("   -> Read  : {0}".format(p_inqueue.qsize()))
                        logger.debug("   -> Write : {0}".format(p_outqueue.qsize()))

                    # Start timers reinit
                    start = time.time()
                    start_idle = None

        except TimeoutError:
            logger.warn('Timeout exception while parsing with %s method', p_process)
            with p_counters['nb_items_error'].get_lock():
                p_counters['nb_items_error'].value += 1
        except KeyboardInterrupt:
            logger.info("user interruption")
            p_inqueue.task_done()
            break
        except Exception as e:
            logger.error("An error occured while processing elements : {0}".format(e))
            main_loop_retry += 1
            if main_loop_retry >= main_loop_max_retry:
                logger.error("Too many errors while processing. Process interrupted after {0} errors".format(main_loop_retry))
                p_inqueue.task_done()
                break
