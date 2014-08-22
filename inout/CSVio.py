from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
import sys
import csv

class CSVio: 
    """Reads and Writes documents from/to csv file"""

    def __init__(self,p_csv_file):
        """Class creation

            Attributes :
            - csvfile:         file cursor
            - out_csvfile:    csv file object
        """
        try:
            self.csvfile = open(p_csv_file, "w")
            self.out_csvfile = csv.writer(self.csvfile,delimiter=';',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
        except:
            logger.error("CSVio : can't open %s for writing",p_csv_file)
            sys.exit(EXIT_IO_ERROR)

    def dequeue_and_store(self,p_queue):
        """Gets docs from p_queue and stores them in the csv file
             Stops dealing with the queue when receiving a "None" item

            p_queue: queue wich items are picked from. Elements has to be "list".
        """
        # Loop untill receiving the "poison pill" item (meaning : no more element to read)
        poison_pill = False
        while not(poison_pill):
            try:
                source_doc = p_queue.get()

                # Manage poison pill : stop trying to get elements
                if source_doc is None:
                    logger.debug("CSVio has received 'poison pill' and is now ending ...")
                    poison_pill = True
                    self.csvfile.close()
                    p_queue.task_done()
                    break

                self.out_csvfile.writerow(source_doc)

                p_queue.task_done()
            except KeyboardInterrupt:
                logger.info("CSVio.dequeue_and_store : User interruption of the process")
                self.csvfile.close()
                sys.exit(EXIT_USER_INTERRUPT)

