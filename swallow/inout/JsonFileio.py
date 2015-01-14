#!/usr/bin/env python
import json
from swallow.settings import logger

class JsonFileio: 
    """Reads all Docs of a Json file and pushes them into a queue"""

    def scan_and_queue(self,p_queue,p_file):
        """ Reads json file and pushes docs to the queue
            If the file contains a list, each doc is pushed in the queue
            If the file contains a doc, the whole doc is pushed in the queue

            p_queue:         Queue where items are pushed to
            p_file:            Json File to scan
        """
        logger.info('Scanning json in %s', p_file)

        # Each items is put into the queue
        try:
            documents = json.load(open(p_file))
        except Exception as e:
            logger.error("Can't read the file %s", p_file)
            logger.error(e)

        if isinstance(documents, list):
            # start_time = datetime.datetime.now()
            for doc in documents:
               p_queue.put(doc)
        else:
            p_queue.put(documents)
