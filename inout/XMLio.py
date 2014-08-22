#!/usr/bin/env python
from swallow.settings import logger, EXIT_IO_ERROR, EXIT_USER_INTERRUPT
import xml.etree.cElementTree as ET
import datetime

class XMLio: 
    """Reads all XML files inside a directory and push XML fragment into a queue"""

    def __init__(self,p_attrib):
        """Attributes"""
        self.bidon = p_attrib        

    def scan_and_queue(self,p_queue,p_file, p_xpath):        
        """Reads xml files in a directory and pushes them to the queue
            
            p_queue:         Queue where items are pushed to
            p_file:            XML File to scan            
            p_xpath:        XPATH used to split document into multiple docs
        """
        logger.info('Scanning xml in %s', p_file)
        start_time = datetime.datetime.now()

        tree = ET.parse(p_file)
        root = tree.getroot()

        # Each items is put into the queue
        compteur = 0
        
        if p_xpath:
             nodeList=root.findall(p_xpath)
        else:
             nodeList=[root]

        for foundElem in nodeList:            
            compteur = compteur + 1            
            #logger.debug("queue size=",p_queue.qsize())
            #logger.debug(ET.tostring(foundElem, encoding="us-ascii", method="xml"))
            try:                
                p_queue.put(ET.tostring(foundElem, encoding="us-ascii", method="xml"))
            except Exception as e:                
                logger.error(e)            
        
        # start_time = datetime.datetime.now()
        # for doc in documents.skip(p_skip).limit(p_skip+p_limit):
        #for doc in documents:
        #    compteur = compteur + 1
            # if compteur % 500 == 0:
            #     elsapsed_time = datetime.datetime.now() - start_time
            #     start_time = datetime.datetime.now()
            #     logger.info('Pushing item number %i in the queue in %s',compteur,elsapsed_time)
            # logger.info('Pushing item number %i in the queue',compteur)
        #    p_queue.put(doc)