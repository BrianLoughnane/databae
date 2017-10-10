from executor.nodeIterator import Iterator

class Scan(Iterator):
    '''
    Wraps a generator to return EOF when it's complete
    '''
    def __init__(self, _input):
        self._input = _input

    def __next__(self):
        try:
            return next(self._input)
        except StopIteration:
            return self.EOF

    def __close__(self):
        pass

    def get_schema(self):
        return next(self)

