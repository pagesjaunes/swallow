from swallow.settings import logger, EXIT_IO_ERROR
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import sys
import time

class Mongoio: 
    """Reads and writes documents from/to a MongoDB Collection"""

    def __init__(self,p_host,p_port,p_base,p_user,p_password,p_connect_timeout=60000,p_rs_xtra_nodes=None,p_rs_name=None):
        """Class creation

            p_host:             Mongo Server address
            p_port:             Mongo Server port
            p_base:             Mongo base
            p_user:             Mongo user
            p_password:         Mongo password
            p_rs_xtra_nodes:    ReplicaSet extra host/port list : ['localhost:27017','myhost.mydom:27018']. The extra hosts/port
                                of the rs (the p_host,p_port is also added to the list)
            p_rs_name:          ReplicaSet Name
        """
        self.host = [p_host+':'+str(p_port)]
        self.base = p_base
        self.user = p_user
        self.password = p_password
        self.connect_timeout = p_connect_timeout
        
        # Add extra hosts:port to the list
        if p_rs_xtra_nodes:
            self.host = self.host + p_rs_xtra_nodes
        if p_rs_name:
            self.rs_name = p_rs_name
        else:
            self.rs_name = None


    def _get_mongo_uri(self):
        """
            Builds the mongo connection uri from the object fields
        """
        r_uri = 'mongodb://%s:%s@%s/%s?connectTimeoutMS=%i' % (self.user,self.password,",".join(self.host),self.base,self.connect_timeout)
        if self.rs_name:
            r_uri = r_uri + "&replicaSet=" + self.rs_name

        return r_uri

    def scan_and_queue(self,p_queue,p_collection,p_query,p_batch_size=100):
        """Reads docs from a collection according to a query and pushes them to the queue

            p_queue:         Queue where items are pushed to
            p_collection:    Collection where items are picked from
            p_query:        MongoDB query for scanning the collection
            p_batch_size:   Number of read docs by iteration
        """
        # uri for mongo connection
        uri = self._get_mongo_uri()

        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            mongo_connect = mongo_client[self.base]
            logger.info('Connection succeeded on %s',uri)
        except PyMongoError as e:
            logger.error('Failed to connect to %s',uri)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        # Scan collection according to the query
        documents = mongo_connect[p_collection].find(p_query)
        nb_docs = documents.count()
        logger.info('Scanning %i items in %s',nb_docs, p_collection)

        # Each items is put into the queue
        documents.batch_size(p_batch_size)

        start_time = time.time()
        for doc in documents:
            p_queue.put(doc)
            # logger.warn('In Queue size : %i',p_queue.qsize())
        time_for_x_items = time.time()        

        if nb_docs == 0:
            logger.info("No document to process")
        else:    
            logger.info("Average reading time : %fs", (time_for_x_items - start_time)/nb_docs)

    def remove_items(self, p_collection, p_query):
        """Execute a delete query on collection using p_query selection              

            p_collection:        mongo collection where to store the docs;            
            p_query:             selection query
        """

        # uri for mongo connection
        uri = self._get_mongo_uri()

        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            mongo_connect = mongo_client[self.base]
            logger.info('Connection succeeded on %s',uri)
        except PyMongoError as e:
            logger.error('Failed to connect to %s',uri)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:            
            mongo_connect[p_collection].remove(p_query)
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

        # uri for mongo connection
        uri = self._get_mongo_uri()

        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            mongo_connect = mongo_client[self.base]
            logger.info('Connection succeeded on %s',uri)
        except PyMongoError as e:
            logger.error('Failed to connect to %s',uri)
            logger.error(e)
            sys.exit(EXIT_IO_ERROR)

        try:            
            return mongo_connect[p_collection].find(p_query).count()
            
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
        uri = self._get_mongo_uri()

        # Connect to mongo
        try:
            mongo_client = MongoClient(uri)
            mongo_connect = mongo_client[self.base]
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
                    mongo_connect[p_collection].update(find,update,upsert=True)
                except Exception as e:
                    logger.error("Document not inserted in Mongo Collection %s", source_doc['_id'])
                    logger.error(e)                

                p_queue.task_done()

            except KeyboardInterrupt:
                logger.info("Mongoio.dequeue_and_store : User interruption of the process")
                sys.exit(EXIT_USER_INTERRUPT)