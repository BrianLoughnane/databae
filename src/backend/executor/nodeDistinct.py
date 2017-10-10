from executor.nodeIterator import Iterator

class Distinct(Iterator):
    def __init__(self, _keys, _input):
        self._input = _input
        self._keys = _keys
        self._last = None

    def __next__(self):
        _next = next(self._input)

        if _next == self.EOF:
            self._input.__close__()
            return self.EOF

        if _next != self._last:
            self._last = _next
            return _next

        return next(self)

    def __close__(self):
        pass

