import datetime
import multiprocessing as mp
from swallow.parser.parser import get_and_parse
from multiprocessing import Process, JoinableQueue, Value, Queue
import logging
from logging.handlers import QueueListener


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

    def __init__(self, p_max_items_by_queue=50000, p_forkserver=False, p_log_every=10000):
        """Class creation"""
        if p_forkserver:
            mp.set_start_method('forkserver')

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

        self.counters = {
            'nb_items_processed': Value('i', 0),
            'nb_items_error': Value('i', 0),
            'nb_items_scanned': Value('i', 0),
            'nb_items_stored': Value('i', 0),
            'whole_storage_time': Value('f', 0),
            'bulk_storage_time': Value('f', 0),
            'whole_process_time': Value('f', 0),
            'real_process_time': Value('f', 0),
            'idle_process_time': Value('f', 0),
            'scan_time': Value('f', 0),
            'log_every': p_log_every
        }

    def run(self, p_processors_nb_threads, p_writer_nb_threads=None):
        # All log messages come and go by this queue
        log_queue = Queue()
        logger = logging.getLogger('swallow')

        if len(logger.handlers) > 1:
            logger.warn("Several handlers detected on swallow logger but can't log to more than a single handler in multiprocessing mode. Only the first one will be used.")
        elif len(logger.handlers) == 0:
            logger.warn("No handler defined for swallow logger. Log to console with info level.")
            # Handler console
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)
            logger.addHandler(stream_handler)

        # each log_listener gets records from the queue and sends them to a specific handler
        handler = logger.handlers[0]
        formatter = handler.formatter
        listener = QueueListener(log_queue, handler)
        listener.start()

        if p_writer_nb_threads is None:
            p_writer_nb_threads = p_processors_nb_threads

        logger.info('Running swallow process. Processor on %i threads / Writers on %i threads', p_processors_nb_threads, p_writer_nb_threads)

        start_time = datetime.datetime.now()

        # Set extra properties to readers
        for reader in self.readers:
            reader['reader'].counters = self.counters
            reader['reader'].log_queue = log_queue
            reader['reader'].log_level = logger.level
            reader['reader'].formatter = formatter

        # Set extra properties to writer
        if self.writer is not None:
            self.writer.counters = self.counters
            self.writer.log_queue = log_queue
            self.writer.log_level = logger.level
            self.writer.formatter = formatter

        read_worker = [Process(target=reader['reader'].scan_and_queue, args=(self.in_queue,), kwargs=(reader['args'])) for reader in self.readers]
        process_worker = [Process(target=get_and_parse, args=(self.in_queue, self.out_queue, self.process, self.counters, log_queue, logger.level, formatter), kwargs=(self.process_args)) for i in range(p_processors_nb_threads)]

        # writers are optionnal
        if self.writer is not None:
            write_worker = [Process(target=self.writer.dequeue_and_store, args=(self.out_queue,), kwargs=(self.writer_store_args)) for i in range(p_writer_nb_threads)]
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
        logger.info('Waiting for reader to finish')
        for work in read_worker:
            # Waiting for all reader to finish their jobs
            work.join()

        # At this point, reading is finished. We had a poison pill for each consumer of read queue :
        for i in range(len(process_worker)):
            self.in_queue.put(None)

        logger.info('Waiting for processors to finish')
        for work in process_worker:
            # Waiting for all processors to finish their jobs
            work.join()

        # At this point, processing is finished. We had a poison pill for each consumer of write queue :
        for i in range(len(write_worker)):
            self.out_queue.put(None)

        logger.info('Waiting for writers to finish')
        for work in write_worker:
            # Waiting for all writers to finish their jobs
            work.join()

        elsapsed_time = datetime.datetime.now() - start_time
        logger.info('Elapsed time : %ss' % elsapsed_time.total_seconds())

        avg_time = 0
        nb_items = self.counters['nb_items_scanned'].value
        if nb_items:
            avg_time = 1000*self.counters['scan_time'].value / nb_items
        logger.info('{0} items scanned ({1}ms)'.format(nb_items, avg_time))

        avg_time = 0
        avg_time_idle = 0
        nb_items = self.counters['nb_items_processed'].value
        if nb_items:
            avg_time = 1000*self.counters['real_process_time'].value / nb_items
            avg_time_idle = 1000*self.counters['idle_process_time'].value / nb_items
        logger.info('{0} items processed (process : {1}ms / idle : {2}ms)'.format(nb_items, avg_time, avg_time_idle))

        avg_time = 0
        nb_items = self.counters['nb_items_stored'].value
        if nb_items:
            avg_time = 1000*self.counters['whole_storage_time'].value / nb_items
        logger.info('{0} items stored ({1}ms)'.format(nb_items, avg_time))

        nb_items = self.counters['nb_items_error'].value
        logger.info('{0} items error'.format(nb_items))

        # Stop listening for log messages
        listener.stop()

    def set_reader(self, p_reader, **kwargs):
        """ Set the reader and its "scan_and_queue" method extra param"""
        self.readers = [{'reader': p_reader, 'args': kwargs}]

    def add_reader(self, p_reader, **kwargs):
        """ Add a reader and its "scan_and_queue" method extra param to the reader list
            May be used when scaning multi data source
        """
        if not self.readers:
            self.set_reader(p_reader, **kwargs)
        else:
            self.readers.append({'reader': p_reader, 'args': kwargs})

    def flush_readers(self):
        """ Removes all the readers """
        del self.readers[:]

    def set_writer(self, p_writer, **kwargs):
        """ Set the writer and its "dequeue_and_store" method extra param"""
        self.writer = p_writer
        self.writer_store_args = kwargs

    def set_process(self, p_process, **kwargs):
        """ Set the processor function and its extra param"""
        self.process = p_process
        self.process_args = kwargs
