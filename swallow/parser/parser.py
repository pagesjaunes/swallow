from multiprocessing import TimeoutError,current_process,Queue
from swallow.settings import logger
import sys

# Get documents from a queue, process them according to a function, and pushes it to a queue
# @param p_inqueue     In queue containing docs to process
# @param p_outqueue Out queue where processed docs are pushed
# @param p_process    function taking a doc as an input and returning a list of docs as a result
def get_and_parse(p_inqueue,p_outqueue,p_process,**kwargs):
    """
        Gets doc from an input queue, applies transformation according to p_process function,
        then pushes the so produced new doc into an output queue

        p_process must take a "doc" as a first parameter
    """

    current = current_process()

    while True:
        try:
            logger.debug("(%s) Size of queues. in : %i / ou : %i",current.name,p_inqueue.qsize(),p_outqueue.qsize())
            
            try:
                in_doc = p_inqueue.get(False)
            except Exception:
                logger.info("Nothing to get in the Queue")
            else:
                # Manage poison pill
                if in_doc is None:
                    logger.info("(%s) => Parser has received 'poison pill' and is now ending ...",current.name)
                    p_inqueue.task_done()
                    break

                # Call the proc with the arg list (keeping the * means : unwrap the list when calling the function)

                out_doc = p_process(in_doc,**kwargs)

                for doc in out_doc:
                    p_outqueue.put(doc)

                p_inqueue.task_done()

        except TimeoutError:
            logger.warn('Timeout exception while parsing with %s method',p_process)
        except KeyboardInterrupt:
            logger.info("user interruption")
            sys.exit(0)
