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


    def clear_index(self,p_index):
        """Deletes and index

            - p_index:     index to delete
            - returns true if p_index has been deleted, false if not
        """
        delete_ok = True

        try:
            client = algoliasearch.Client(self.app_id,self.api_key)
            index = client.init_index(p_index)
            index.clear_index()
            logger.info('Index %s deleted',p_index)
        except Exception as e:
            logger.error('Error deleting the index %s',p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok


    def delete_document(self,p_index,p_id):
        """Deletes a doc from an index
            - p_index:      index where to delete the doc
            - p_id:         id of the doc to delete
        """
        delete_ok = True

        try:
            client = algoliasearch.Client(self.app_id,self.api_key)
            index = client.init_index(p_index)
            index.delete_object(p_id)
            logger.info('%s deleted from %s',p_id, p_index)
        except Exception as e:
            logger.error('Error deleting the %s from index %s',p_id, p_index)
            logger.error(e)
            delete_ok = False

        return delete_ok


    def set_settings(self,p_index,p_conf):
        """Sets the index settings
        """
        try:
            client = algoliasearch.Client(self.app_id,self.api_key)
            index = client.init_index(p_index)
            index.set_settings(p_conf)
            logger.info('Index %s set',p_index)
        except Exception as e:
            logger.error('Error setting the index %s',p_index)
            logger.error(e)


    def get_settings(self,p_index):
        """Gets the index settings
        """
        try:
            client = algoliasearch.Client(self.app_id,self.api_key)
            index = client.init_index(p_index)
            result = index.get_settings()
            logger.info('Index %s get',p_index)
            return result
        except Exception as e:
            logger.error('Error getting settings of %s',p_index)
            logger.error(e)


    def dequeue_and_store(self,p_queue,p_index,p_nbmax_retry=3):
        """Gets docs from p_queue and stores them in the algolia
             Stops dealing with the queue when receiving a "None" item

            p_queue:             queue wich items are picked from. Elements has to be "list".
            p_index:            algolia index where to store the docs
            p_nbmax_retry:      number of tries when failing on a request (default is 3)
        """

        client = algoliasearch.Client(self.app_id,self.api_key)
        index = client.init_index(p_index)

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

                try_counter = 1
                is_indexed = False
                while try_counter <= p_nbmax_retry and not is_indexed:
                    try:
                        # Bulk indexation
                        if len(bulk) > 0:
                            logger.debug("Indexing %i documents",len(bulk))
                            index.add_objects(bulk)
                    except Exception as e:
                        logger.error("Bulk not indexed in algolia - Retry number %i",try_counter)
                        logger.error(e)
                        try_counter += 1
                    else:
                        is_indexed = True

                if not is_indexed:
                    logger.error("Bulk not indexed in algolia : operation aborted after %i retries",try_counter-1)

            except KeyboardInterrupt:
                logger.info("ESio.dequeue_and_store : User interruption of the process")
                sys.exit(1)

    def scan_and_queue(self, p_queue, p_index, p_query={}, p_connect_timeout=1, p_read_timeout=30):
        """Reads docs from an Algolia index according to a query and pushes them to the queue

            p_queue:        Queue where items are pushed to
            p_index:        Index where items are picked from
            p_query:        query for scanning the index
        """
        try:
            client = algoliasearch.Client(self.app_id,self.api_key)
            client.timeout = (p_connect_timeout, p_read_timeout)
            index = client.init_index(p_index)
        except Exception as e:
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:
            documents = index.browse_all(p_query)
            
            for doc in documents:
                p_queue.put(doc)
        except Exception as e:
            logger.info("Error while scanning Algolia index %s with query %s",p_index,p_query)
