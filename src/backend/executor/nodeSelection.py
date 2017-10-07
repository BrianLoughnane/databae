from functools import partial

from executor.nodeIterator import Iterator
from operators import operator_map

class Selection(Iterator):
    '''
    select * from movies where id = 5;

    movies_file = FileScan('movies.csv')
    predicate = lambda m: m.id = 5
    Selection(movies_file, predicate)
    '''
    def __init__(self, _predicate, _input):
        self._input = _input
        self._predicate = _predicate

    def _get_next(self):
        _next = self._input.__next__()

        if _next == self.EOF:
            self._input.__close__()
            return self.EOF

        if self._predicate(_next):
            return _next

        return self._get_next()

    def __next__(self):
        return self._get_next()

    def __close__(self):
        pass

    @staticmethod
    def parse_args(schema, args):
        # parse plan language into individual condition tuples
        plan_string = ','.join(args)
        conditions = [
            condition_string.split(',') for condition_string
            in plan_string.split(',AND,')
        ]

        # map condition tuples into Operator objects
        operators = []
        for condition in conditions:
            operand1 = condition[0]
            operator_key = condition[1]
            operand2 = condition[2]

            operator_class = operator_map.get(operator_key)
            operator = operator_class(operand1, operand2)

            operators.append(operator)

        # return a function that predicates all operators
        def master_predicate(schema, row):
            for operator in operators:
                if operator.check(schema, row) == False:
                    return False
            return True

        return partial(master_predicate, schema)

