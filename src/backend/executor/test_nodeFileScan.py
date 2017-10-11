import unittest

from executor.nodeFileScan import FileScan

FILE_PATH = 'test_files/sample_movies.csv'

class TestFileScan(unittest.TestCase):
    def setUp(self):
        self._list = [
            ['movieId', 'title', 'genres'],
            ['1','Toy Story (1995)', 'Adventure|Animation|Children|Comedy|Fantasy'],
            ['2','Jumanji (1995)', 'Adventure|Children|Fantasy'],
            ['3', 'Grumpier Old Men (1995)', 'Comedy|Romance'],
        ]
        self._input = FILE_PATH
        self._with_comma = ['11', 'American President, The (1995)','Comedy|Drama|Romance']

    def test_init(self):
        instance = FileScan(self._input)
        self.assertTrue(instance._file)
        self.assertTrue(instance.reader)

    def test_next(self):
        instance = FileScan(self._input)

        self.assertEquals(
          next(instance),
          self._list[0]
        )

        self.assertEquals(
          next(instance),
          self._list[1]
        )

        self.assertEquals(
          next(instance),
          self._list[2]
        )
        next(instance) # 3
        next(instance) # 4
        next(instance) # 5
        next(instance) # 6
        next(instance) # 7
        next(instance) # 8
        next(instance) # 9
        next(instance) # 10

        self.assertEquals(
          next(instance),
          self._with_comma
        )

    def test_close(self):
        instance = FileScan(self._input)
        self.assertEquals(instance.__close__(), None)

