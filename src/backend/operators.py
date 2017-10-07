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

operator_map = {
    'EQUALS': Equals,
    'LESS_THAN': LessThan,
    'GREATER_THAN': GreaterThan,
}

