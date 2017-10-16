def b():
    print('Hello')
    yield 1

def f(ready=False):
    if ready:
        return b()
    else:
        return f(True)


if __name__ == '__main__':
    ret = f()
    print(ret)
    print(type(ret))
    print(next(ret))

