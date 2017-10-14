from collections import defaultdict

from executor.nodeIterator import Iterator
from executor.nodeSort import Sort

class HashJoin(Iterator):
    '''
    HashJoin only supports joining on equality.

    HashJoin streams one input fully into memory and stores
    it in a hash table.

    It then streams items from then next table until a matching
    hash is found, returning each matching tuple in that
    hash bucket each time next() is called.
    '''
    def __init__(
          self,
          get_input1_join_columns, get_input2_join_columns,
    ):
        '''
        Assumes sorted input streams
        '''
        super().__init__()

        self.get_input1_join_columns = get_input1_join_columns
        self.get_input2_join_columns = get_input2_join_columns

        self._iterable = self.get_iterable()

    def initiate_hash_table(self):
        '''
        Drains first input into memory, storing it in a
        hash table, which is keyed on the join column
        of the first table.

        Assumes first input is the smaller relation.
        '''
        self.hash_table = defaultdict(list)
        _input1 = self._inputs[0]
        for record1 in _input1:
            if record1 == self.EOF:
                break
            key = self.get_input1_join_columns(record1)
            self.hash_table[key].append(record1)

    def get_iterable(self):
        '''
        Lazily drains the second input, looking up it's join
        columns in the hash table and producing matching tuples
        from the hash table.
        '''
        if not hasattr(self, 'hash_table'):
            self.initiate_hash_table()

        for record2 in self._inputs[1]:
            if record2 == self.EOF:
                yield self.EOF

            key = self.get_input2_join_columns(record2)

            match_list = self.hash_table.get(key, [])
            for record1 in match_list:
                yield record1 + record2

    def __next__(self):
        return next(self._iterable)

    def __close__(self):
        pass

    @staticmethod
    def parse_args(schema, args):
        # TODO
        pass

