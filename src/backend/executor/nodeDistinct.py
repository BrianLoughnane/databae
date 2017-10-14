from executor.nodeIterator import Iterator

class Distinct(Iterator):
    def __init__(self, _keys):
        super().__init__()
        self._keys = _keys
        self._last = None

    def __next__(self):
        _input = self._inputs[0]
        _next = next(_input)

        if _next == self.EOF:
            _input.__close__()
            return self.EOF

        if _next != self._last:
            self._last = _next
            return _next

        return next(self)

    def __close__(self):
        pass

