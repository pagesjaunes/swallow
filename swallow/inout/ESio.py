from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
from elasticsearch import Elasticsearch, helpers, NotFoundError
import json
import sys
import time
from swallow.logger_mp import get_logger_mp


class ESio:
    """Reads and Writes documents from/to elasticsearch"""

    def __init__(self, p_host, p_port, p_bulksize):
        """Class creation

            p_host:     Elasticsearch Server address
            p_port:        Elasticsearch Server port
            p_bulksize:    Number of doc to index in a time
        """
        self.host = p_host
        self.port = p_port
        self.bulk_size = p_bulksize

    def count(self, p_index, p_query={}):
        """Gets the number of docs for a query

            p_index:    elasticsearch index where to query
            p_query:    the query to process

            return the number of docs from the index p_index and the query p_query
        """
        try:
            param = [{'host': self.host, 'port': self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s', json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s', json.dumps(param))
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:
            result = es.count(index=p_index, body=p_query)
            logger.info('Count the number of items from %s for the query %s', p_index, p_query)
        except Exception as e:
            logger.error('Error querying the index %s with query %s', p_index, p_query)
            logger.error(e)

        return result['count']

    def set_mapping(self, p_index, p_mapping):
        """Create an index with a given p_mapping

            - p_index:     index to delete
            - p_mapping:   mapping forced
        """
        try:
            param = [{'host': self.host, 'port': self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s', json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s', json.dumps(param))
            logger.error(e)

        try:
            es.indices.create(index=p_index, body=p_mapping)
            logger.info('Index %s created', p_index)
        except Exception as e:
            logger.error('Error creating the index %s', p_index)
            logger.error(e)

    def clear_index(self, p_index):
        """Deletes and index

            - p_index:     index to delete
            - returns true if p_index has been deleted, false if not
        """
        delete_ok = True

        try:
            param = [{'host': self.host, 'port': self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s', json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s', json.dumps(param))
            logger.error(e)
            delete_ok = False

        try:
            es.indices.delete(index=p_index)
            logger.info('Index %s deleted', p_index)
        except Exception as e:
            logger.error('Error deleting the index %s', p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok

    def dequeue_and_store(self, p_queue, p_index, p_timeout=10, p_nbmax_retry=3, p_disable_indexing=False):
        """Gets docs from p_queue and stores them in the csv file
             Stops dealing with the queue when receiving a "None" item

            p_queue:            queue wich items are picked from. Elements has to be "list".
            p_index:            elasticsearch index where to store the docs
            p_timeout:          timeout for bulk (default is 10s)
            p_nbmax_retry:      number of tries when failing on a request (default is 3)
        """
        logger_mp = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)

        try:
            param = [{'host': self.host, 'port': self.port, 'timeout': p_timeout, 'max_retries': p_nbmax_retry, 'retry_on_timeout': True}]
            es = Elasticsearch(param)
            logger_mp.info('Connected to ES Server: %s', json.dumps(param))
        except Exception as e:
            logger_mp.error('Connection failed to ES Server : %s', json.dumps(param))
            logger_mp.error(e)
            sys.exit(EXIT_IO_ERROR)

        if p_disable_indexing:
            self._set_indexing_refresh(logger_mp, es, p_index, "-1")

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
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

                    # Bulk element creation from the source_doc
                    source_doc['_index'] = p_index

                    bulk.append(source_doc)
                    p_queue.task_done()

                try_counter = 1
                is_indexed = False
                while try_counter <= p_nbmax_retry and not is_indexed:
                    start_bulking = time.time()

                    try:
                        # Bulk indexation
                        if len(bulk) > 0:
                            helpers.bulk(es, bulk, raise_on_error=True)
                    except Exception as e:
                        logger_mp.error("Bulk not indexed in ES - Retry n°{0}".format(try_counter))
                        logger_mp.error(e)
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
                            if nb_items % self.counters['log_every'] == 0:
                                logger_mp.info("Store : {0} items".format(nb_items))
                                logger_mp.debug("   -> Avg store time : {0}ms".format(1000*self.counters['whole_storage_time'].value / nb_items))
                                logger_mp.debug("   -> Avg bulk time  : {0}ms".format(1000*self.counters['bulk_storage_time'].value / nb_items))

                            start = time.time()

                if not is_indexed:
                    start = time.time()
                    logger_mp.error("Bulk not indexed in elasticsearch : operation aborted after %i retries", try_counter-1)
                    with self.counters['nb_items_error'].get_lock():
                        self.counters['nb_items_error'].value += len(bulk)

            except KeyboardInterrupt:
                logger_mp.info("ESio.dequeue_and_store : User interruption of the process")
                # If indexing has been disabled, enable it again
                if p_disable_indexing:
                    self._set_indexing_refresh(logger_mp, es, p_index, "1s")

                sys.exit(EXIT_USER_INTERRUPT)

        # If indexing has been disabled, enable it again
        if p_disable_indexing:
            self._set_indexing_refresh(logger_mp, es, p_index, "1s")

    def scan_and_queue(self, p_queue, p_index, p_query={}, p_doctype=None, p_scroll_time='5m', p_timeout='1m', p_size=100, p_overall_timeout=30, p_nbmax_retry=3):
        """Reads docs from an es index according to a query and pushes them to the queue

            p_queue:         Queue where items are pushed to
            p_scroll_time:    Time for scroll method
            p_timeout:        Timeout - After this period, scan context is closed
            p_index:        Index where items are picked from
            p_doctype:        DocType of the items
            p_query:        ElasticSearch query for scanning the index
        """
        logger_mp = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)

        try:
            param = [{'host': self.host, 'port': self.port, 'timeout': p_overall_timeout, 'max_retries': p_nbmax_retry, 'retry_on_timeout': True}]
            es = Elasticsearch(param)
            logger_mp.info('Connected to ES Server for reading: %s', json.dumps(param))
        except Exception as e:
            logger_mp.error('Connection failed to ES Server for reading: %s', json.dumps(param))
            logger_mp.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:
            if 'p_doctype' is not None:
                documents = helpers.scan(client=es, query=p_query, size=p_size, scroll=p_scroll_time, index=p_index, doc_type=p_doctype, timeout=p_timeout)
            else:
                documents = helpers.scan(client=es, query=p_query, size=p_size, scroll=p_scroll_time, index=p_index, timeout=p_timeout)

            start = time.time()
            for doc in documents:
                p_queue.put(doc)

                elapsed = time.time() - start

                with self.counters['nb_items_scanned'].get_lock():
                    self.counters['nb_items_scanned'].value += 1
                    nb_items = self.counters['nb_items_scanned'].value
                    self.counters['scan_time'].value += elapsed

                    if nb_items % self.counters['log_every'] == 0:
                        logger_mp.info("Scan : {0} items".format(nb_items))
                        logger_mp.debug("   -> Avg scan time : {0}ms".format(1000*self.counters['scan_time'].value / nb_items))

                    # Start timers reinit
                    start = time.time()

        except Exception as e:
            logger_mp.info("Error while scanning ES index %s with query %s", p_index, p_query)
            with self.counters['nb_items_error'].get_lock():
                self.counters['nb_items_error'].value += 1

    def _set_indexing_refresh(self, p_logger, p_es_client, p_index, p_refresh_rate="1s"):
        """
            Set the refresh rate for a given index
        """
        if p_refresh_rate == "-1":
            p_logger.warn("Indexing disabled for index {0}".format(p_index))
        else:
            p_logger.warn("Indexing enabled for index {0}".format(p_index))

        # Enable indexing again
        settings = {
            "index": {
                "refresh_interval": p_refresh_rate
            }
        }

        if not p_es_client.indices.exists(index=p_index):
            try:
                p_es_client.indices.create(index=p_index, body={'settings': settings})
            except Exception as e:
                p_logger.info("Can't create index {0}, already exists ?".format(p_index))
                p_logger.info(e)
        else:
            p_es_client.indices.put_settings(body=settings, index=p_index)
