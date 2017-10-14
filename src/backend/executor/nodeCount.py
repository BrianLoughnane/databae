from executor.nodeIterator import Iterator

class Count(Iterator):
    def __init__(self):
        super().__init__()
        self.finished = False

    def __next__(self):
        if self.finished:
            return self.EOF

        count = self.get_count()
        self.finished = True
        return count

    def get_count(self):
        _input = self._inputs[0]

        count = 0
        for _next in _input:
            if _next == self.EOF:
                break
            count += 1
        return (count,)

