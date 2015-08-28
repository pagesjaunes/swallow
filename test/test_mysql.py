"""
    Test of mysql connector
"""

from swallow.inout.Mysqlio import Mysqlio
from multiprocessing import JoinableQueue

def test_basic():
    in_queue = JoinableQueue()

    mysql_reader = Mysqlio('localhost','3600','test','root','') 
    mysql_reader.scan_and_queue(in_queue,"SELECT * FROM swallow")

    assert in_queue.qsize() == 3

    res = []
    while not in_queue.empty():
        res.append(in_queue.get())

    expected_res = [{'id':1,'libelle':'test'},{'id':2,'libelle':'john'},{'id':3,'libelle':'woo'}]

    assert res == expected_res
