from executor.nodeIterator import Iterator

class Scan(Iterator):
    '''
    Wraps a generator to return EOF when it's complete
    '''
    def __init__(self, values, pop_headers=False):
        super().__init__()
        self.values = values
        self.reset(pop_headers=pop_headers)

    def __next__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            return self.EOF

    def __close__(self):
        pass

    def reset(self, pop_headers=False):
        self._iterator = (ii for ii in self.values)
        if pop_headers:
            next(self._iterator)

