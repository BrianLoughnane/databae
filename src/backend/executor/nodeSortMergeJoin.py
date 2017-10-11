from executor.nodeIterator import Iterator
from executor.nodeSort import Sort

class SortMergeJoin(Iterator):
    def __init__(
        self,
        theta,
        _input1, _input2,
    ):
        '''
        SortMergeJoin can only join on a single condition.
        Assumes sorted input streams.
        '''
        self.theta = theta

        self._input1 = _input1
        self._input2 = _input2

        self.buffers = {
            self._input1: next(self._input1),
            self._input2: next(self._input2),
        }

        self._iterable = self.get_iterable()


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

        # sort pairs by value (record)
        # TODO the value needs to be projected
        # to the join column, otherwise we don't know
        # which column to sort on
        sorted_items = sorted(
          filtered_input_record_pairs,
          key=lambda input__record: input__record[1]
        )

        try:
            min_pair = sorted_items.pop(0)
        except IndexError:
            return None

        min_input, min_value = min_pair

        self.buffers[min_input] = next(min_input)

    def get_iterable(self):
        if not self.buffers:
            yield self.EOF

        record1 = self.buffers.get(self._input1, None)
        record2 = self.buffers.get(self._input2, None)

        while True:
            if not self.buffers:
                break

            if not self.theta(record1, record2):
                self.pop_lowest_buffer()
                continue

            nested_first_records = [record1]
            nested_second_records = [record2]

            # build up repeated records from both inputs
            while True:
                record1 = next(self._input1)
                # TODO == is wrong.  equality is
                # determined by join column
                if record1 == nested_first_records[0]:
                    nested_first_records.append(record1)
                else:
                    break

            while True:
                record2 = next(self._input2)
                # TODO == is wrong.  equality is
                # determined by join column
                if record2 == nested_first_records[0]:
                    nested_first_records.append(record2)
                else:
                    break

            # nested loops over duplicate records
            for nested_record1 in nested_first_records:
                for nested_record2 in nested_second_records:
                    yield nested_record1 + nested_record2

    def __next__(self):
        return next(self._iterable)

    def __close__(self):
        pass

    @staticmethod
    def parse_args(schema, args):
        # TODO
        pass
