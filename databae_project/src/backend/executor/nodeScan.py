from src.backend.executor.nodeIterator import Iterator

class Scan(Iterator):
  EOF = 'end of fun'

  def __next__(self):
    try:
        return self._input.__next__()
    except StopIteration:
        return self.EOF

