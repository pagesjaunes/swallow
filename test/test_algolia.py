"""
    Test of algolia reader connector
"""

from swallow.inout.Algoliaio import Algoliaio
from multiprocessing import JoinableQueue

def test_basic():
    in_queue = JoinableQueue()

    algolia_reader = Algoliaio("7TM6BC4HX4", "2b8d141963b0b2d12b7a67b79853900b", 1000)
    algolia_reader.scan_and_queue(in_queue, p_index="INT_Rubriques",p_query=None, p_connect_timeout=30, p_read_timeout=60)

    assert in_queue.qsize() > 2600