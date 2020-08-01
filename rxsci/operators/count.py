from .scan import scan


def count(reduce=False):
    return scan(lambda acc, i: acc + 1, 0, reduce=reduce)
