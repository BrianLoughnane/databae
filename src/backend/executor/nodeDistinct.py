from executor.nodeIterator import Iterator

class Distinct(Iterator):
    def __init__(self, _keys):
        self._keys = _keys
        self._last = None

    def __next__(self):
        _next = next(self.inputs[0])

        if _next == self.EOF:
            self._input.__close__()
            return self.EOF

        if _next != self._last:
            self._last = _next
            return _next

        return next(self)

    def __close__(self):
        pass

