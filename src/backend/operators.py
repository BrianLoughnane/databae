class Operator():
    isEquals = False
    isLessThan = False
    isGreaterThan = False

    def __init__(self, operand1, operand2, schema):
        '''
        operand1 - column name like 'id'
        operand2 - value to be compared against
        '''
        self.operand1 = operand1
        self.operand2 = operand2
        self.schema = schema

    def check(self, row):
        operand1__index = self.schema.index(self.operand1)
        operand1__value = row[operand1__index]

        return self.operator(operand1__value, self.operand2)

    def get_value(self):
        return self.operand2

    def operator(self):
        raise ValueError('Not implemented')

class Equals(Operator):
    isEquals = True
    operator = lambda _s, a, b: a == b

class LessThan(Operator):
    isLessThan = True
    operator = lambda _s, a, b: a < b

class GreaterThan(Operator):
    isGreaterThan = True
    operator = lambda _s, a, b: a > b

operator_map = {
    'EQUALS': Equals,
    'LESS_THAN': LessThan,
    'GREATER_THAN': GreaterThan,
}

