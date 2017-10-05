import unittest
from src.backend.executor.nodeIterator import Iterator

class TestIterator(unittest.TestCase):
    def test_init(self):
        self.assertEquals(Iterator().init(), None)

    def test_next(self):
        self.assertEquals(Iterator().next(), None)

    def test_close(self):
        self.assertEquals(Iterator().close(), None)
