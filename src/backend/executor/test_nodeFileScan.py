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
        self._with_comma = ['11', '"American President, The (1995)"','Comedy|Drama|Romance']

    def test_init(self):
        instance = FileScan(self._input)
        self.assertEquals(instance.file_name, self._input)

    def test_next(self):
        instance = FileScan(self._input)
        self.assertEquals(
          instance.__next__(),
          self._list[0]
        )

        self.assertEquals(
          instance.__next__(),
          self._list[1]
        )

        self.assertEquals(
          instance.__next__(),
          self._list[2]
        )
        instance.__next__() # 3
        instance.__next__() # 4
        instance.__next__() # 5
        instance.__next__() # 6
        instance.__next__() # 7
        instance.__next__() # 8
        instance.__next__() # 9
        instance.__next__() # 10

        self.assertEquals(
          instance.__next__(),
          self._with_comma
        )

    def test_close(self):
        instance = FileScan(self._input)
        self.assertEquals(instance.__close__(), None)

