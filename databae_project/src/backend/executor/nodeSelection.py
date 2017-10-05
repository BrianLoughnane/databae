from src.backend.executor.nodeScan import Scan

class Selection(Scan):
  def __init__(self, _input, _predicate):
    super().__init__(_input)
    self._predicate = _predicate

  def _get_next(self):
    _next = super().__next__()

    if _next is self.EOF:
      return self.EOF
    elif self._predicate(_next):
      return _next
    else:
      return self._get_next()

  def __next__(self):
    return self._get_next()

