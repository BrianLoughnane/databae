import unittest

from executor.nodeIterator import Iterator

class TestIterator(unittest.TestCase):
    def test_init(self):
        class Subclass(Iterator):
            pass

        with self.assertRaises(ValueError):
            Subclass()

    def test_next(self):
        class Subclass(Iterator):
            def  __init__(self):
                pass

        instance = Subclass()
        with self.assertRaises(ValueError):
            instance.__next__()

    def test_close(self):
        class Subclass(Iterator):
            def  __init__(self):
                pass

        instance = Subclass()
        with self.assertRaises(ValueError):
            instance.__close__()

