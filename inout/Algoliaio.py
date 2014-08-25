from swallow.settings import logger
from algoliasearch import algoliasearch
import sys

class Algoliaio: 
    """Reads and Writes documents from/to algolia"""
    
    def __init__(self,p_app_id,p_api_key,p_bulksize):
        """Class creation

            p_app_id:   Algolia app id
            p_api_key:  Algolia api key
            p_bulksize: Number of doc to index in a time
        """
        self.app_id = p_app_id
        self.api_key = p_api_key
        self.bulk_size = p_bulksize


    # def clear_index(self,p_index):
    #     """Deletes and index

    #         - p_index:     index to delete
    #         - returns true if p_index has been deleted, false if not
    #     """
    #     delete_ok = True

    #     try:
    #         param = [{'host':self.host,'port':self.port}]
    #         es = Elasticsearch(param)
    #         logger.info('Connected to ES Server: %s',json.dumps(param))
    #     except Exception as e:
    #         logger.error('Connection failed to ES Server : %s',json.dumps(param))
    #         logger.error(e)
    #         delete_ok = False

    #     try:
    #         es.indices.delete(index=p_index)
    #         logger.info('Index %s deleted',p_index)
    #     except Exception as e:
    #         logger.error('Error deleting the index %s',p_index)
    #         logger.error(e)
    #         delete_ok = False

    #     return delete_ok

    def dequeue_and_store(self,p_queue,p_index):
        """Gets docs from p_queue and stores them in the algolia
             Stops dealing with the queue when receiving a "None" item

            p_queue:             queue wich items are picked from. Elements has to be "list".
            p_index:            algolia index where to store the docs
        """
        client = algoliasearch.Client(self.app_id,self.api_key)
        index = client.initIndex(p_index)

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

                    bulk.append(source_doc)
                    p_queue.task_done()

                try:
                    # Bulk indexation
                    if len(bulk) > 0:
                        logger.info("Indexing %i documents",len(bulk))
                        res = index.addObjects(bulk)
                        logger.info("Task id %s", res)
                except Exception as e:
                    logger.error("Bulk not indexed in algolia")
                    logger.error(e)
            except KeyboardInterrupt:
                logger.info("ESio.dequeue_and_store : User interruption of the process")
                sys.exit(1)

    # def scan_and_queue(self,p_queue,p_index,p_query={},p_doctype=None,p_scroll_time='60m',p_timeout='60m'):
    #     """Reads docs from an es index according to a query and pushes them to the queue

    #         p_queue:         Queue where items are pushed to
    #         p_scroll_time:    Time for scroll method
    #         p_timeout:        Timeout - After this period, scan context is closed
    #         p_index:        Index where items are picked from
    #         p_doctype:        DocType of the items
    #         p_query:        ElasticSearch query for scanning the index
    #     """
    #     try:
    #         param = [{'host':self.host,'port':self.port}]
    #         es = Elasticsearch(param)
    #         logger.info('Connected to ES Server for reading: %s',json.dumps(param))
    #     except Exception as e:
    #         logger.error('Connection failed to ES Server for reading: %s',json.dumps(param))
    #         logger.error(e)
    #         sys.exit(EXIT_IO_ERROR)

    #     if 'p_doctype' is not None:
    #         documents = helpers.scan(client=es, query=p_query, scroll=p_scroll_time, index=p_index, doc_type=p_doctype, timeout=p_timeout)
    #     else:
    #         documents = helpers.scan(client=es, query=p_query, scroll= p_scroll_time, index=p_index, timeout=p_timeout)
    #     for doc in documents:
    #         logger.debug(doc)
    #         p_queue.put(doc)
