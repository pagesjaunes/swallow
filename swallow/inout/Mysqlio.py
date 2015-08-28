from swallow.settings import logger, EXIT_IO_ERROR
import sys
import time
import pymysql.cursors


class Mysqlio: 
    """Reads documents from a Mysql DB"""

    def __init__(self,p_host,p_port,p_base,p_user,p_password):
        """Class creation

            p_host:        Mysql Server address
            p_port:        Mysql Server port
            p_base:        Mysql base
            p_user:        Mysql user
            p_password:    Mysql password
        """
        self.host = p_host
        self.port = p_port
        self.base = p_base
        self.user = p_user
        self.password = p_password

    def scan_and_queue(self,p_queue,p_query):
        """Reads docs according to a query and pushes them to the queue

            p_queue:         Queue where items are pushed to
            p_query:        MongoDB query for scanning the collection
        """

        connection = pymysql.connect(host=self.host,
                             user=self.user,
                             password=self.password,
                             db=self.base,
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                cursor.execute(p_query)
                for row in cursor:
                    p_queue.put(row)
                cursor.close()
        finally:
            connection.close()
