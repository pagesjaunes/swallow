from swallow.settings import logger, EXIT_IO_ERROR
import sys
import time
import pymysql.cursors
from pymysql import OperationalError


class Mysqlio:
    """Reads documents from a Mysql DB"""

    def __init__(self, p_host, p_port, p_base, p_user, p_password):
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

    def scan_and_queue(self, p_queue, p_query, p_bulksize=1000, p_start=0):
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
            offset = p_start
            stop = False
            # delete ";" if set at the end of the query
            query = p_query
            if query.endswith(';'):
                query = query[:-1]
            with connection.cursor() as cursor:
                while not stop:
                    paginated_query = "{0} limit {1},{2}".format(p_query, offset, p_bulksize)
                    logger.debug("MySqlIo : Start dealing with records from {0} to {1}".format(offset, p_bulksize + offset))
                    try:
                        cursor.execute(paginated_query)
                    except pymysql.OperationalError as e:
                        logger.error("MySqlIo : Error while dealing with records from {0} to {1}".format(offset, p_bulksize + offset))
                        logger.error(e)
                        raise e
                    if cursor.rowcount:
                        for row in cursor:
                            p_queue.put(row)
                        offset += p_bulksize
                    else:
                        stop = True
                    logger.debug("MySqlIo : All records from {0} to {1} has been put in the queue".format(offset, p_bulksize + offset))
                cursor.close()
        finally:
            connection.close()
