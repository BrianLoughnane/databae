from itertools import starmap

from executor.nodeIterator import Iterator
from executor.nodeSort import Sort

class SortMergeJoin(Iterator):
    def __init__(
        self,
        theta,
        projection1, projection2):
        '''
        SortMergeJoin can only join on a single condition.
        Assumes sorted input streams.
        '''
        super().__init__()

        self.theta = theta

        self.projection1 = projection1
        self.projection2 = projection2

        self.initialized = False

    def initialize(self):
        self._input1 = self._inputs[0]
        self._input2 = self._inputs[1]

        # pop off headers
        #TODO - handle this elsewhere
        next(self._input1)
        next(self._input2)

        self.projectors = {
            self._input1: self.projection1,
            self._input2: self.projection2,
        }

        self.buffers = {
            self._input1: next(self._input1),
            self._input2: next(self._input2),
        }

        self._iterable = self.get_iterable()

        self.initialized = True

    def pop_lowest_buffer(self):
        '''
        Removes the lowest of the in memory values
        from the buffer and replaces it with
        the next value of the input that it came from.
        '''
        input_record_pairs = list(self.buffers.items())

        # remove EOFs
        filtered_input_record_pairs = list(filter(
            lambda t: t[1] != self.EOF,
            input_record_pairs
        ))

        projected_input_record_pairs = \
          list(starmap(lambda _input, record:
              (_input, self.projectors[_input](record)),
            filtered_input_record_pairs,
          ))

        sorted_items = sorted(
          projected_input_record_pairs,
          key=lambda input__record: input__record[1],
        )

        try:
            min_pair = sorted_items.pop(0)
        except IndexError:
            return None

        min_input, _  = min_pair
        self.buffers[min_input] = next(min_input)

    def get_next_from_buffer(self, _input):
      _next = next(_input)
      self.buffers[_input] = _next
      return _next

    def buffer_has_values(self):
        return bool(list(filter(
          lambda v: v is not self.EOF,
          self.buffers.values()
        )))

    def get_iterable(self):
        if not self.buffer_has_values():
            yield self.EOF

        record1 = self.buffers.get(self._input1)
        record2 = self.buffers.get(self._input2)

        while True:
            if not self.buffer_has_values():
                if self.theta(record1, record2):
                    yield record1 + record2
                break

            # if condition not passing, pop off lowest
            if not self.theta(record1, record2):
                print('%s does not match %s' % (
                    record1, record2
                ))
                self.pop_lowest_buffer()
                continue

            nested_first_records = []
            nested_second_records = []

            # build up records from both inputs that are
            # of the same sort key
            while True:
                nested_first_records.append(record1)
                sort_key1 = self.projection1(record1)

                # reassign record1 variable to next
                record1 = self.get_next_from_buffer(
                  self._input1,
                )
                if record1 == self.EOF:
                    break

                # if new record1 has the same sort key, append it + repeat
                if self.projection1(record1) == sort_key1:
                    continue
                else:
                    break

            while True:
                nested_second_records.append(record2)
                sort_key2 = self.projection2(record2)

                # reassign record2 variable to next
                record2 = self.get_next_from_buffer(
                  self._input2,
                )
                if record2 == self.EOF:
                    break

                # if new record2 has the same sort key, append it + repeat
                if self.projection2(record2) == sort_key2:
                    continue
                else:
                    break

            # nested loops over duplicate records
            for nested_record1 in nested_first_records:
                for nested_record2 in nested_second_records:
                    if self.theta(
                        nested_record1,
                        nested_record2,
                    ):
                        yield nested_record1 + nested_record2

    def __next__(self):
        if not self.initialized:
            self.initialize()
        try:
            return next(self._iterable)
        except StopIteration:
            return self.EOF

    def __close__(self):
        pass

    @staticmethod
    def parse_args(schema, args):
        # TODO
        pass
