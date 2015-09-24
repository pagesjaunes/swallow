from swallow.settings import logger, EXIT_USER_INTERRUPT
import sys
import csv
import os

class CSVio: 
    """Reads and Writes documents from/to csv file"""

    def __init__(self):
        # Contains all file cursors
        self.csvfilecursor = {}
        self.out_csvfile = {}

    def dequeue_and_store(self,p_queue,p_file,p_delimiter=',',p_quotechar='"',p_quoting=csv.QUOTE_NONNUMERIC):
        """Gets docs from p_queue and stores them in the csv file
             Stops dealing with the queue when receiving a "None" item

            p_queue:    queue wich items are picked from. Elements has to be "list".
            p_file:     file to store in
        """

        # If not exists, creates the cursor
        if p_file not in self.csvfilecursor:
            self.csvfilecursor[p_file] = open(p_file, "w")
            self.out_csvfile[p_file] = csv.writer(self.csvfilecursor[p_file],delimiter=p_delimiter,quotechar=p_quotechar,quoting=p_quoting,lineterminator=os.linesep)

        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
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

                p_queue.task_done()
            except KeyboardInterrupt:
                logger.info("CSVio.dequeue_and_store : User interruption of the process")
                self.csvfilecursor[p_file].close()
                sys.exit(EXIT_USER_INTERRUPT)

    def scan_and_queue(self,p_queue,p_file,p_delimiter=',',p_skip_header=True):        
        """Reads csv file and pushes each line to the queue
            
            p_queue:    Queue where items are pushed to
            p_file:     CSV File to scan
            p_skip_header: Don't pass the first line
        """
        logger.info('Scanning csv in %s', p_file)

        # cr = csv.reader(open(p_file,"r",delimiter=p_delimiter))
        # for row in cr:
        #     print (row)

        filecursor = open(p_file, 'r')
        reader = csv.reader(filecursor,delimiter=p_delimiter)

        # Skip first line ?
        skipline = p_skip_header
        for row in reader:
            if not skipline:
                p_queue.put(row)
            else:
                skipline = False
