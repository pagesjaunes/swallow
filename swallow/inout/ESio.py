from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
from elasticsearch import Elasticsearch, helpers
import json
import sys
import time

class ESio: 
    """Reads and Writes documents from/to elasticsearch"""
    
    def __init__(self,p_host,p_port,p_bulksize):
        """Class creation

            p_host:     Elasticsearch Server address
            p_port:        Elasticsearch Server port
            p_bulksize:    Number of doc to index in a time
        """
        self.host = p_host
        self.port = p_port
        self.bulk_size = p_bulksize

    def count(self,p_index,p_query={}):
        """Gets the number of docs for a query

            p_index:    elasticsearch index where to query
            p_query:    the query to process

            return the number of docs from the index p_index and the query p_query
        """
        try:
            param = [{'host':self.host,'port':self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s',json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s',json.dumps(param))
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:
            result = es.count(index=p_index,body=p_query)
            logger.info('Count the number of items from %s for the query %s',p_index,p_query)
        except Exception as e:
            logger.error('Error querying the index %s with query %s',p_index,p_query)
            logger.error(e)

        return result['count']

    def set_mapping(self,p_index,p_mapping):
        """Create an index with a given p_mapping

            - p_index:     index to delete
            - p_mapping:   mapping forced
        """
        try:
            param = [{'host':self.host,'port':self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s',json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s',json.dumps(param))
            logger.error(e)

        try:
            es.indices.create(index=p_index,body=p_mapping)
            logger.info('Index %s created',p_index)
        except Exception as e:
            logger.error('Error creating the index %s',p_index)
            logger.error(e)

    def clear_index(self,p_index):
        """Deletes and index

            - p_index:     index to delete
            - returns true if p_index has been deleted, false if not
        """
        delete_ok = True

        try:
            param = [{'host':self.host,'port':self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s',json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s',json.dumps(param))
            logger.error(e)
            delete_ok = False

        try:
            es.indices.delete(index=p_index)
            logger.info('Index %s deleted',p_index)
        except Exception as e:
            logger.error('Error deleting the index %s',p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok

    def dequeue_and_store(self,p_queue,p_index,p_timeout=10,p_nbmax_retry=3):
        """Gets docs from p_queue and stores them in the csv file
             Stops dealing with the queue when receiving a "None" item

            p_queue:            queue wich items are picked from. Elements has to be "list".
            p_index:            elasticsearch index where to store the docs
            p_timeout:          timeout for bulk (default is 10s)
            p_nbmax_retry:      number of tries when failing on a request (default is 3)
        """
        try:
            param = [{'host':self.host,'port':self.port,'timeout':p_timeout,'max_retries':p_nbmax_retry,'retry_on_timeout':True}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server: %s',json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server : %s',json.dumps(param))
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
        poison_pill = False
        while not(poison_pill):
            try:
                bulk = []
                while (len(bulk) < self.bulk_size):
                    source_doc = p_queue.get()
                    
                    # Manage poison pill
                    if source_doc is None:
                        logger.debug("ESio has received 'poison pill' and is now ending ...")
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
                    try:
                        # Bulk indexation
                        if len(bulk) > 0:
                            logger.debug("Indexing %i documents",len(bulk))
                            helpers.bulk(es, bulk, raise_on_error=True)
                            # es.index(index=self.index,doc_type=p_doctype,body=source_doc)
                    except Exception as e:
                        logger.error("Bulk not indexed in ES - Retry n°%i",try_counter)
                        logger.error(e)
                        try_counter += 1
                    else:
                        is_indexed = True

                if not is_indexed:
                    logger.error("Bulk not indexed in elasticsearch : operation aborted after %i retries",try_counter-1)                  

            except KeyboardInterrupt:
                logger.info("ESio.dequeue_and_store : User interruption of the process")
                sys.exit(EXIT_USER_INTERRUPT)

    def scan_and_queue(self,p_queue,p_index,p_query={},p_doctype=None,p_scroll_time='5m',p_timeout='1m'):
        """Reads docs from an es index according to a query and pushes them to the queue

            p_queue:         Queue where items are pushed to
            p_scroll_time:    Time for scroll method
            p_timeout:        Timeout - After this period, scan context is closed
            p_index:        Index where items are picked from
            p_doctype:        DocType of the items
            p_query:        ElasticSearch query for scanning the index
        """
        try:
            param = [{'host':self.host,'port':self.port}]
            es = Elasticsearch(param)
            logger.info('Connected to ES Server for reading: %s',json.dumps(param))
        except Exception as e:
            logger.error('Connection failed to ES Server for reading: %s',json.dumps(param))
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:
            if 'p_doctype' is not None:
                documents = helpers.scan(client=es, query=p_query, size=1000, scroll=p_scroll_time, index=p_index, doc_type=p_doctype, timeout=p_timeout)
            else:
                documents = helpers.scan(client=es, query=p_query, size=1000, scroll= p_scroll_time, index=p_index, timeout=p_timeout)
            for doc in documents:
                logger.debug(doc)
                p_queue.put(doc)
        except Exception as e:
            logger.info("Error while scanning ES index %s with query %s",p_index,p_query)
