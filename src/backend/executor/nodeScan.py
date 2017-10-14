from executor.nodeIterator import Iterator

class Scan(Iterator):
    '''
    Wraps a generator to return EOF when it's complete
    '''
    def __init__(self, values):
        super().__init__()
        self.values = values
        self.reset()

    def __next__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            return self.EOF

    def __close__(self):
        pass

    def reset(self):
        self._iterator = (ii for ii in self.values)

