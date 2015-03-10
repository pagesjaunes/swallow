from swallow.settings import logger, EXIT_IO_ERROR
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import sys
import time

class Mongoio: 
    """Reads and writes documents from/to a MongoDB Collection"""

    def __init__(self,p_host,p_port,p_base,p_user,p_password):
        """Class creation

            p_host:     Mongo Server address
            p_port:        Mongo Server port
            p_base:        Mongo base
            p_user:        Mongo user
            p_password:    Mongo password
        """
        self.host = p_host
        self.port = p_port
        self.base = p_base
        self.user = p_user
        self.password = p_password

        # uri for mongo connection
        uri = 'mongodb://%s:%s@%s:%s/%s' % (self.user,self.password,self.host,self.port,self.base)
        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            self.mongo = mongo_client[self.base]
            logger.info('Connection succeeded on %s',uri)
        except PyMongoError as e:
            logger.error('Failed to connect to %s',uri)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

    def scan_and_queue(self,p_queue,p_collection,p_query):
        """Reads docs from a collection according to a query and pushes them to the queue

            p_queue:         Queue where items are pushed to
            p_collection:    Collection where items are picked from
            p_query:        MongoDB query for scanning the collection
        """

        # Scan collection according to the query
        documents = self.mongo[p_collection].find(p_query)
        nb_docs = documents.count()
        logger.info('Scanning %i items in %s',nb_docs, p_collection)

        # Each items is put into the queue
        documents.batch_size(100)

        # time_for_x_items = 0
        # num_items_processed = 0
        # num_items_average = 1000
        start_time = time.time()
        for doc in documents:
            p_queue.put(doc)
            # logger.warn('In Queue size : %i',p_queue.qsize())
        time_for_x_items = time.time()
        # num_items_processed += 1
        # if (num_items_processed % num_items_average) == 0:
        #     logger.info("Average reading time : %fs (after %i items)", time_for_x_items/num_items_processed, num_items_processed)

        logger.info("Average reading time : %fs", (time_for_x_items - start_time)/nb_docs)

    def remove_items(self, p_collection, p_query):
        """Execute a delete query on collection using p_query selection              

            p_collection:        mongo collection where to store the docs;            
            p_query:             selection query
        """
        try:            
            self.mongo[p_collection].remove(p_query)
            logger.info('Collection items removal done')
        except PyMongoError as e:
            logger.error('Failed to remove entries from %s',p_collection)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

    def count_items(self, p_collection, p_query):
        """Return item count using p_query selection              

            p_collection:        mongo collection to query;            
            p_query:             selection query
        """
        try:            
            return self.mongo[p_collection].find(p_query).count()
            
        except PyMongoError as e:
            logger.error('Failed to count entries from %s',p_collection)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

    def dequeue_and_store(self,p_queue, p_collection):
        """Gets docs from p_queue and stores them in a mongo collection
             Stops dealing with the queue when receiving a "None" item

            p_queue:             queue wich items are picked from. Elements has to be "list".
            p_collection:        mongo collection where to store the docs;            
        """
        # uri for mongo connection
        uri = 'mongodb://%s:%s@%s:%s/%s' % (self.user,self.password,self.host,self.port,self.base)
        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            mongo_connection = mongo_client[self.base]
            logger.info('Connection succeeded on %s',uri)
        except PyMongoError as e:
            logger.error('Failed to connect to %s',uri)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
        poison_pill = False        

        while not(poison_pill):
            try:                
                
                source_doc = p_queue.get()

                # Manage poison pill
                if source_doc is None:
                    logger.debug("Mongoio has received 'poison pill' and is now ending ...")
                    poison_pill = True
                    p_queue.task_done()
                    break

                #management of 'update/set' style request                 
                try:
                    find = source_doc['_mongo_find']
                except KeyError:
                    find = {'_id':source_doc['_id']}

                try:
                    update = source_doc['_mongo_update']
                except KeyError:
                    update = source_doc
            
                #insert into collection
                try:                                                                        
                    mongo_connection[p_collection].update(find,update,upsert=True)
                except Exception as e:
                    logger.error("Document not inserted in Mongo Collection %s", source_doc['_id'])
                    logger.error(e)                

                p_queue.task_done()

            except KeyboardInterrupt:
                logger.info("Mongoio.dequeue_and_store : User interruption of the process")
                sys.exit(EXIT_USER_INTERRUPT)