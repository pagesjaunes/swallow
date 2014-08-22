from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
from elasticsearch import Elasticsearch, helpers
import json
import sys

class ESio: 
	"""Reads and Writes documents from/to elasticsearch"""
	
	def __init__(self,p_host,p_port,p_bulksize):
		"""Class creation

			p_host: 	Elasticsearch Server address
			p_port:		Elasticsearch Server port
			p_bulksize:	Number of doc to index in a time
		"""
		self.host = p_host
		self.port = p_port
		self.bulk_size = p_bulksize

	def clear_index(self,p_index):
		"""Deletes and index

			- p_index: 	index to delete
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

	def dequeue_and_store(self,p_queue,**kwargs):
		"""Gets docs from p_queue and stores them in the csv file
		 	Stops dealing with the queue when receiving a "None" item

			p_queue: 			queue wich items are picked from. Elements has to be "list".
			p_index:			elasticsearch index where to store the docs
			p_doctype:			doctype of the indexed docs
			p_id_field_name:	if a field of the doc has to be used as the elasticsearch '_id', set it here
		"""
		try:
			param = [{'host':self.host,'port':self.port}]
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
					document = {
						"_index": kwargs['p_index'],
						"_type": kwargs['p_doctype'],
						"_source": source_doc
					}

					# If a field is defined as the id one, report it as the ElasticSearch _id
					if 'p_id_field_name' in kwargs:
						id_field_name = kwargs['p_id_field_name']
						if id_field_name in source_doc:
							document["_id"]=source_doc[id_field_name]

					bulk.append(document)
					p_queue.task_done()

				try:
					# Bulk indexation
					if len(bulk) > 0:
						logger.debug("Indexing %i documents",len(bulk))
						helpers.bulk(es, bulk)
						# es.index(index=self.index,doc_type=p_doctype,body=source_doc)
				except Exception as e:
					logger.error("Bulk not indexed in ES")
					logger.error(e)
			except KeyboardInterrupt:
				logger.info("ESio.dequeue_and_store : User interruption of the process")
				sys.exit(EXIT_USER_INTERRUPT)

	def scan_and_queue(self,p_queue,**kwargs):
		"""Reads docs from an es index according to a query and pushes them to the queue

			p_queue: 	Queue where items are pushed to
			p_index:	Index where items are picked from
			p_doctype:	DocType of the items
			p_query:	ElasticSearch query for scanning the index
		"""
		try:
			param = [{'host':self.host,'port':self.port}]
			es = Elasticsearch(param)
			logger.info('Connected to ES Server: %s',json.dumps(param))
		except Exception as e:
			logger.error('Connection failed to ES Server : %s',json.dumps(param))
			logger.error(e)
			sys.exit(EXIT_IO_ERROR)

		if 'p_doctype' in kwargs:
			documents = helpers.scan(client= es, query=kwargs['p_query'], scroll= "10m", index=kwargs['p_index'], doc_type=kwargs['p_doctype'], timeout="10m")
		else:
			documents = helpers.scan(client= es, query=kwargs['p_query'], scroll= "10m", index=kwargs['p_index'], timeout="10m")
		# documents= helpers.scan(client= self.es, query=p_query, scroll= "10m", index=p_index, doc_type=p_doctype, timeout="10m")
		for doc in documents:
			p_queue.put(doc)
