import csv
import os
from swallow.logger_mp import get_logger_mp


class CSVio:
    """Reads and Writes documents from/to csv file"""

    def __init__(self):
        # Contains all file cursors
        self.csvfilecursor = {}
        self.out_csvfile = {}

    def dequeue_and_store(self, p_queue, p_file, p_delimiter=',', p_quotechar='"', p_quoting=csv.QUOTE_NONNUMERIC):
        """Gets docs from p_queue and stores them in the csv file
             Stops dealing with the queue when receiving a "None" item

            p_queue:    queue wich items are picked from. Elements has to be "list".
            p_file:     file to store in
        """
        logger = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)

        # If not exists, creates the cursor
        if p_file not in self.csvfilecursor:
            self.csvfilecursor[p_file] = open(p_file, "w")
            self.out_csvfile[p_file] = csv.writer(self.csvfilecursor[p_file], delimiter=p_delimiter, quotechar=p_quotechar, quoting=p_quoting, lineterminator=os.linesep)

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
        # Main loop max retry
        main_loop_max_retry = 5
        main_loop_retry = 0
        poison_pill = False
        while not(poison_pill):
            try:
                source_doc = p_queue.get()

                # Manage poison pill : stop trying to get elements
                if source_doc is None:
                    logger.debug("CSVio has received 'poison pill' and is now ending ...")
                    poison_pill = True
                    self.csvfilecursor[p_file].close()
                    p_queue.task_done()
                    break

                self.out_csvfile[p_file].writerow(source_doc)
                with self.counters['nb_items_stored'].get_lock():
                    self.counters['nb_items_stored'].value += 1
                    if self.counters['nb_items_stored'].value % self.counters['log_every'] == 0:
                        logger.info("Storage in progress : {0} items written to target".format(self.counters['nb_items_stored'].value))

                p_queue.task_done()
            except KeyboardInterrupt:
                logger.info("CSVio.dequeue_and_store : User interruption of the process")
                self.csvfilecursor[p_file].close()
                poison_pill = True
                p_queue.task_done()
            except Exception as e:
                logger.error("An error occured while storing elements to CSV : {0}".format(e))
                main_loop_retry += 1
                if main_loop_retry >= main_loop_max_retry:
                    logger.error("Too many errors while storing. Process interrupted after {0} errors".format(main_loop_retry))
                    poison_pill = True
                    p_queue.task_done()

    def scan_and_queue(self, p_queue, p_file, p_delimiter=',', p_skip_header=True):
        """Reads csv file and pushes each line to the queue

            p_queue:    Queue where items are pushed to
            p_file:     CSV File to scan
            p_skip_header: Don't pass the first line
        """
        logger = get_logger_mp(__name__, self.log_queue, self.log_level, self.formatter)
        logger.info('Scanning csv in %s', p_file)

        filecursor = open(p_file, 'r')
        reader = csv.reader(filecursor, delimiter=p_delimiter)

        # Skip first line ?
        skipline = p_skip_header
        for row in reader:
            if not skipline:
                p_queue.put(row)
                with self.counters['nb_items_scanned'].get_lock():
                    self.counters['nb_items_scanned'].value += 1
                    if self.counters['nb_items_scanned'].value % self.counters['log_every'] == 0:
                        logger.info("Scan in progress : {0} items read from source".format(self.counters['nb_items_scanned'].value))
            else:
                skipline = False
