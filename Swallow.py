import datetime
from swallow.parser.parser import get_and_parse
from multiprocessing import Process, JoinableQueue
from swallow.settings import logger

class Swallow:
    """ This Class allows data multiprocessing
        It runs multiple workers that communicate with two queues
            - Readers get data from a source and push it into the "in_queue"
            - Processors get data from "in_queue", transform it and push it into the "out_queue"
            - Writers get data from "out_queue" and store it into a destination base or file

        - Readers are objects defining a "scan_and_queue" method according to the following signature : scan_and_queue(self,p_queue,**kwargs)
            This typically scans a base/file/collection/index and puts each document/item into the p_queue
        - Writers are objects defining a "dequeue_and_store" method according to the following signature : dequeue_and_store(self,p_queue,**kwargs)
            This typically gets documents/items from a queue and stores them into a base/file/collection/index
        - Processors are functions that transform a reader format doc into the writer expected format. They must have the signature :
            function_name(p_srcDoc,*args) and must return a list of doc in the expected format

        Note that as they consume a queue, both writers and processors must deal the "poison pill" item. Once they get a "None" item from
        the list they are consuming, they must stop to listen to it.
        The Swallow object automatically generates these "pills" as it knows when producers have finished their task.

        Example : 

            # Transforms a doc from the es index to a csv row
            def create_csv_row(p_srcdoc,*args):
                csv_row = []
                csv_row.append(p_srcdoc['field_for_col1'])
                csv_row.append(p_srcdoc['field_for_col2'])
                return [csv_row]

            nb_threads =5
            es_reader = ESio('127.0.0.1','9200',1000)
            csv_writer = CSVio(arguments['--csv'])

            swal = Swallow()
            swal.set_reader(es_reader,p_index='my_es_index',p_doctype='my_doc_type',p_query={})
            swal.set_writer(csv_writer)
            swal.set_process(create_csv_row)

            swal.run(nb_threads)
    """

    def __init__(self, p_max_items_by_queue=50000):
        """Class creation"""

        self.readers = None
        self.writer = None
        self.writer_store_args = None
        self.process = None
        self.process_args = None
        if p_max_items_by_queue is None:
            self.in_queue = JoinableQueue()
            self.out_queue = JoinableQueue()
        else:
            self.in_queue = JoinableQueue(p_max_items_by_queue)
            self.out_queue = JoinableQueue(p_max_items_by_queue)

    def run(self,p_processors_nb_threads,p_writer_nb_threads=None):
        if p_writer_nb_threads is None:
            p_writer_nb_threads = p_processors_nb_threads

        logger.info('Running swallow process. Processor on %i threads / Writers on %i threads',p_processors_nb_threads,p_writer_nb_threads)

        start_time = datetime.datetime.now()

        read_worker = [Process(target=reader['reader'].scan_and_queue, args=(self.in_queue,),kwargs=(reader['args'])) for reader in self.readers]
        process_worker = [Process(target=get_and_parse, args=(self.in_queue,self.out_queue,self.process),kwargs=(self.process_args)) for i in range(p_processors_nb_threads)]

        # writers are optionnal
        if self.writer is not None:
            write_worker = [Process(target=self.writer.dequeue_and_store, args=(self.out_queue,),kwargs=(self.writer_store_args)) for i in range(p_writer_nb_threads)]
        else:
            write_worker = []

        # Running workers
        for work in read_worker:
            work.start()
        for work in process_worker:
            work.start()
        for work in write_worker:
            work.start()
        
        # Waiting for workers to end :
        # worker.join() blocks the programm till the worker ends
        for work in read_worker:
            # Waiting for all reader to finish their jobs
            logger.info('Waiting for reader to finish')
            work.join()

        # At this point, reading is finished. We had a poison pill for each consumer of read queue :
        for i in range(len(process_worker)):
            self.in_queue.put(None)

        for work in process_worker:
            # Waiting for all processors to finish their jobs
            logger.info('Waiting for processors to finish')
            work.join()

        # At this point, processing is finished. We had a poison pill for each consumer of write queue :
        for i in range(len(write_worker)):
            self.out_queue.put(None)

        for work in write_worker:
            # Waiting for all writers to finish their jobs
            logger.info('Waiting for writers to finish')
            work.join()

        elsapsed_time = datetime.datetime.now() - start_time
        logger.info('Elapsed time : %ss' % elsapsed_time.total_seconds())

    def set_reader(self,p_reader,**kwargs):
        """ Set the reader and its "scan_and_queue" method extra param"""
        self.readers = [{'reader':p_reader,'args':kwargs}]

    def add_reader(self,p_reader,**kwargs):
        """ Add a reader and its "scan_and_queue" method extra param to the reader list
            May be used when scaning multi data source
        """
        if not self.readers:
            self.set_reader(p_reader,**kwargs)
        else:
            self.readers.append({'reader':p_reader,'args':kwargs})

    def flush_readers(self):
        """ Removes all the readers """
        del self.readers[:]

    def set_writer(self,p_writer,**kwargs):
        """ Set the writer and its "dequeue_and_store" method extra param"""
        self.writer = p_writer
        self.writer_store_args = kwargs

    def set_process(self,p_process,**kwargs):
        """ Set the processor function and its extra param"""
        self.process = p_process
        self.process_args = kwargs
