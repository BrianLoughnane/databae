from executor.nodeIterator import Iterator

class Sort(Iterator):
    def __init__(self, _keys, _input):
        self._input = _input
        self._keys = _keys
        self._items = self._get_items()
        self._sorted_items = sorted(self._items)
        self._index = 0

    def _get_items(self):
        values = []
        while True:
            _next = self._input.__next__()
            if _next == Iterator.EOF:
                self._input.__close__()
                break
            values.append(_next)
        return values

    def _get_next_sorted_item(self):
        try:
            value = self._sorted_items[self._index]
            self._index += 1
            return value
        except IndexError:
            self._input.__close__()
            return self.EOF

    def __next__(self):
        return self._get_next_sorted_item()

    def __close__(self):
        pass

