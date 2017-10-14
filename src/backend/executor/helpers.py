from executor.nodeIterator import Iterator

class Print(Iterator):
    def __init__(self, null):
        super().__init__()

    def __next__(self):
        _input = self._inputs[0]
        _next = next(_input)
        print(str(_input), _next)
        return _next

class Debug(Iterator):
    def __init__(self, null):
        super().__init__()

    def __next__(self):
        if len(self._inputs) == 1:
            _input = self._inputs[0]
            _next = next(_input)
        if len(self._inputs) == 2:
            _input1 = self._inputs[0]
            _input2 = self._inputs[1]
            _next1 = next(_input)
            _next2 = next(_input)
        import ipdb; ipdb.set_trace();
        return _next

