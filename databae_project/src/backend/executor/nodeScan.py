from src.backend.executor.nodeIterator import Iterator

class Scan(Iterator):
    '''
    Wraps a generator to return EOF when it's complete

    sequence = Scan('filename')
    '''
    def __init__(self, _input):
        self._input = _input

    def __next__(self):
        try:
            return self._input.__next__()
        except StopIteration:
            return self.EOF

    def __close__(self):
        pass

class FileScan(Iterator):
    '''
    Opens up a file and passes bytes of the file
    up to the parent from it's .next() method

    my_table = Scan('filename')
    '''
    def __init__(self, file_name):
        with open(file_name, 'r') as _file:
            self._file = _file

    def __next__(self):
        try:
            return self._file.read(1000)
        except StopIteration:
            return self.EOF

    def __close__(self):
        self._file.close()

