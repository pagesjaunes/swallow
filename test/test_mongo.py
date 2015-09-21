"""
    Test of mongo connector
"""

from swallow.inout.Mongoio import Mongoio
from multiprocessing import JoinableQueue

def test_basic():
    in_queue = JoinableQueue()

    mongo_reader = Mongoio(p_host='localhost',p_port='27017',p_user='activite',p_password='passactivite',p_base='ACTIVITE',p_rs_xtra_nodes=['localhost:27018','localhost:27019'],p_rs_name='rs0')
    mongo_reader.scan_and_queue(in_queue,p_collection='rubriques', p_query={})

    assert in_queue.qsize() > 2600

