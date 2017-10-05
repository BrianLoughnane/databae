from src.backend.executor.nodeIterator import Iterator

EOF = 'end of fun'

class Scan(Iterator):
  def __init__(self, _input):
    self._input = _input

  def next(self):
    try:
        return self._input.__next__()
    except StopIteration:
        return EOF
        self.close()

