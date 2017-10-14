import unittest

from executor.nodeIterator import Iterator

class TestIterator(unittest.TestCase):
    def test_init(self):
        class Subclass(Iterator):
            pass
        instance = Subclass()
        self.assertTrue(hasattr(instance, '_inputs'))

    def test_next(self):
        class Subclass(Iterator):
            def  __init__(self):
                pass

        instance = Subclass()
        with self.assertRaises(ValueError):
            next(instance)

