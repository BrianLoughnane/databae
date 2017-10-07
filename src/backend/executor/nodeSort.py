from executor.nodeIterator import Iterator

class Sort(Iterator):
    def __init__(self, _sorts, _input):
        self._input = _input

        self._items = self._get_items()
        for _sort in _sorts:
          self._items = sorted(self._items, key=_sort)

          # for now, only supports sorting by a single key
          break

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
            value = self._items[self._index]
            self._index += 1
            return value
        except IndexError:
            self._input.__close__()
            return self.EOF

    def __next__(self):
        return self._get_next_sorted_item()

    def __close__(self):
        pass

    @staticmethod
    def parse_args(schema, args):
        schema_indices = [
            schema.index(arg)
            for arg in args
        ]

        return [
          lambda row: row[schema_index]
          for schema_index in schema_indices
        ]

