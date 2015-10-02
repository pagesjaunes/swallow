""" This package allows data multiprocessing
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

            from swallow import Swallow
            # Transforms a doc from the es index to a csv row
            def create_csv_row(p_srcdoc,*args):
                csv_row = []
                csv_row.append(p_srcdoc['field_for_col1'])
                csv_row.append(p_srcdoc['field_for_col2'])
                return [csv_row]

            nb_threads = 5
            es_reader = ESio('127.0.0.1','9200',1000)
            csv_writer = CSVio(arguments['--csv'])

            swal = Swallow()
            swal.set_reader(es_reader,p_index='my_es_index',p_doctype='my_doc_type',p_query={})
            swal.set_writer(csv_writer)
            swal.set_process(create_csv_row)

            swal.run(nb_threads)
    """

__version__ = "1.4.4"
