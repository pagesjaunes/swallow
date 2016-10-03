from swallow.settings import logger
from algoliasearch import algoliasearch
from swallow.logger_mp import get_logger_mp
import time


class Algoliaio:
    """Reads and Writes documents from/to algolia"""

    def __init__(self, p_app_id, p_api_key, p_bulksize):
        """Class creation

            p_app_id:   Algolia app id
            p_api_key:  Algolia api key
            p_bulksize: Number of doc to index in a time
        """
        self.app_id = p_app_id
        self.api_key = p_api_key
        self.bulk_size = p_bulksize

    def clear_index(self, p_index):
        """Deletes and index

            - p_index:     index to delete
            - returns true if p_index has been deleted, false if not
        """
        delete_ok = True

        try:
            client = algoliasearch.Client(self.app_id, self.api_key)
            index = client.init_index(p_index)
            index.clear_index()
            logger.info('Index %s deleted', p_index)
        except Exception as e:
            logger.error('Error deleting the index %s', p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok

    def delete_document(self, p_index, p_id):
        """Deletes a doc from an index
            - p_index:      index where to delete the doc
            - p_id:         id of the doc to delete
        """
        delete_ok = True

        try:
            client = algoliasearch.Client(self.app_id, self.api_key)
            index = client.init_index(p_index)
            index.delete_object(p_id)
            logger.info('%s deleted from %s', p_id, p_index)
        except Exception as e:
            logger.error('Error deleting the %s from index %s', p_id, p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok

    def set_settings(self, p_index, p_conf):
        """Sets the index settings
        """
        try:
            client = algoliasearch.Client(self.app_id, self.api_key)
            index = client.init_index(p_index)
            index.set_settings(p_conf)
            logger.info('Index %s set', p_index)
        except Exception as e:
            logger.error('Error setting the index %s', p_index)
            logger.error(e)

    def get_settings(self, p_index):
        """Gets the index settings
        """
        try:
            client = algoliasearch.Client(self.app_id, self.api_key)
            index = client.init_index(p_index)
            result = index.get_settings()
            logger.info('Index %s get', p_index)
            return result
        except Exception as e:
            logger.error('Error getting settings of %s', p_index)
            logger.error(e)

    def dequeue_and_store(self, p_queue, p_index, p_nbmax_retry=3):
        """Gets docs from p_queue and stores them in the algolia
             Stops dealing with the queue when receiving a "None" item

            p_queue:             queue wich items are picked from. Elements has to be "list".
            p_index:            algolia index where to store the docs
            p_nbmax_retry:      number of tries when failing on a request (default is 3)
        """
        logger = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)

        client = algoliasearch.Client(self.app_id, self.api_key)
        index = client.init_index(p_index)

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
        # Main loop max retry
        main_loop_max_retry = 5
        main_loop_retry = 0
        start = time.time()
        poison_pill = False
        while not(poison_pill):
            try:
                bulk = []
                while (len(bulk) < self.bulk_size):
                    source_doc = p_queue.get()

                    # Manage poison pill
                    if source_doc is None:
                        poison_pill = True
                        p_queue.task_done()
                        break

                    bulk.append(source_doc)
                    p_queue.task_done()

                try_counter = 1
                is_indexed = False
                while try_counter <= p_nbmax_retry and not is_indexed:
                    start_bulking = time.time()
                    try:
                        # Bulk indexation
                        if len(bulk) > 0:
                            index.add_objects(bulk)
                    except Exception as e:
                        logger.error("Bulk not indexed in algolia - Retry number %i", try_counter)
                        logger.error(e)
                        try_counter += 1
                    else:
                        is_indexed = True
                        now = time.time()
                        elapsed_bulking = now - start_bulking
                        elapsed = now - start
                        with self.counters['nb_items_stored'].get_lock():
                            self.counters['nb_items_stored'].value += len(bulk)
                            self.counters['whole_storage_time'].value += elapsed
                            self.counters['bulk_storage_time'].value += elapsed_bulking
                            nb_items = self.counters['nb_items_stored'].value
                            if nb_items % self.counters['log_every'] == 0 and nb_items != 0:
                                logger.info("Store : {0} items".format(nb_items))
                                logger.debug("   -> Avg store time : {0}ms".format(1000 * self.counters['whole_storage_time'].value / nb_items))
                                logger.debug("   -> Avg bulk time  : {0}ms".format(1000 * self.counters['bulk_storage_time'].value / nb_items))

                            start = time.time()

                if not is_indexed:
                    start = time.time()
                    logger.error("Bulk not indexed in algolia : operation aborted after %i retries", try_counter - 1)
                    with self.counters['nb_items_error'].get_lock():
                        self.counters['nb_items_error'].value += len(bulk)

            except KeyboardInterrupt:
                logger.info("ESio.dequeue_and_store : User interruption of the process")
                poison_pill = True
                p_queue.task_done()
            except Exception as e:
                logger.error("An error occured while storing elements to Algolia : {0}".format(e))
                main_loop_retry += 1
                if main_loop_retry >= main_loop_max_retry:
                    logger.error("Too many errors while storing. Process interrupted after {0} errors".format(main_loop_retry))
                    poison_pill = True
                    p_queue.task_done()

    def scan_and_queue(self, p_queue, p_index, p_query={}, p_connect_timeout=1, p_read_timeout=30):
        """Reads docs from an Algolia index according to a query and pushes them to the queue

            p_queue:        Queue where items are pushed to
            p_index:        Index where items are picked from
            p_query:        query for scanning the index
        """
        logger = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)
        try:
            client = algoliasearch.Client(self.app_id, self.api_key)
            client.timeout = (p_connect_timeout, p_read_timeout)
            index = client.init_index(p_index)
        except Exception as e:
            logger.error(e)

        try:
            documents = index.browse_all(p_query)
            start = time.time()
            for doc in documents:
                p_queue.put(doc)
                elapsed = time.time() - start

                with self.counters['nb_items_scanned'].get_lock():
                    self.counters['nb_items_scanned'].value += 1
                    nb_items = self.counters['nb_items_scanned'].value
                    self.counters['scan_time'].value += elapsed

                    if nb_items % self.counters['log_every'] == 0:
                        logger.info("Scan : {0} items".format(nb_items))
                        logger.debug("   -> Avg scan time : {0}ms".format(1000 * self.counters['scan_time'].value / nb_items))

                    # Start timers reinit
                    start = time.time()
        except Exception as e:
            logger.info("Error while scanning Algolia index %s with query %s", p_index, p_query)
            with self.counters['nb_items_error'].get_lock():
                self.counters['nb_items_error'].value += 1
