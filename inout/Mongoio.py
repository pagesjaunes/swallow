from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class Mongoio: 
	"""Reads and writes documents from/to a MongoDB Collection"""

	def __init__(self,p_host,p_port,p_base,p_user,p_password):
		"""Class creation

			p_host: 	Mongo Server address
			p_port:		Mongo Server port
			p_base:		Mongo base
			p_user:		Mongo user
			p_password:	Mongo password
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

			p_queue: 		Queue where items are pushed to
			p_collection:	Collection where items are picked from
			p_query:		MongoDB query for scanning the collection
		"""

		# Scan collection according to the query
		documents = self.mongo[p_collection].find(p_query)
		nb_docs = documents.count()
		logger.info('Scanning %i items in %s',nb_docs, p_collection)

		# Each items is put into the queue
		compteur = 0
		documents.batch_size(100)

		for doc in documents:
			compteur = compteur + 1
			del doc['_id']	# Mongo ObjectId can't be pushed in a Queue
			p_queue.put(doc)

