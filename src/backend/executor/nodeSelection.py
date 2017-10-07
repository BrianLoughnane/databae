from functools import partial

from executor.nodeIterator import Iterator

class Operator():
    def __init__(self, operand1__name, operand2__name):
        self.operand1__name = operand1__name
        self.operand2__name = operand2__name

    def check(self, schema, row):
        operand1__index = schema.index(self.operand1__name)
        operand1__value = row[operand1__index]
        operand2__value = self.operand2__name

        return self.operator(operand1__value, operand2__value)

    def operator(self):
        raise ValueError('Not implemented')

class Equals(Operator):
    operator = lambda _s, a, b: a == b

class LessThan(Operator):
    operator = lambda _s, a, b: a < b

class GreaterThan(Operator):
    operator = lambda _s, a, b: a > b


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
        _schema = self._input.schema

        if _next == self.EOF:
          self._input.__close__()
          return self.EOF

        elif self._predicate(_schema, _next):
            return _next

        else:
            return self._get_next()

    def __next__(self):
        return self._get_next()

    def __close__(self):
        pass

    @staticmethod
    def parse_args(args):
        operator_map = {
            'EQUALS': Equals,
            'LESS_THAN': LessThan,
            'GREATER_THAN': GreaterThan,
        }

        as_string = ','.join(args)
        conditions = [
            condition_string.split(',') for condition_string
            in as_string.split(',AND,')
        ]

        operators = []
        for condition in conditions:
            operand1 = condition[0]
            operator_key = condition[1]
            operand2 = condition[2]

            operator_class = operator_map.get(operator_key)
            operator = operator_class(operand1, operand2)

            operators.append(operator)

        def master_predicate(schema, row):
            for operator in operators:
                if operator.check(schema, row) == False:
                    return False
            return True

        return master_predicate

