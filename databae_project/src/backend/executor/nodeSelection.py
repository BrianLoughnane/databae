from src.backend.executor.nodeIterator import Iterator

class Selection(Iterator):
    '''
    select * from movies where id = 5;

    movies_file = FileScan('movies.csv')
    predicate = lambda m: m.id = 5
    Selection(movies_file, predicate)
    '''
    def __init__(self, _input, _predicate):
        self._input = _input
        self._predicate = _predicate

    def _get_next(self):
        _next = self._input.__next__()
        if _next == self.EOF:
          return self.EOF
        elif self._predicate(_next):
            return _next
        else:
            return self._get_next()

    def __next__(self):
        return self._get_next()

    def __close__(self):
        pass

