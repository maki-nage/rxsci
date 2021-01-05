from .scan import scan


def count(reduce=False):
    """Counts the number of items emitted in the source Observable.

    .. marble::
        :alt: count

        --4--1--2--1--|
        [     count()      ]
        --1--2--3--4--|

    .. marble::
        :alt: count

        --4--1--2--1--|
        [count(reduce=True)]
        --------------4-|

    Args:
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the number of source items that have been
        emitted.
    """
    return scan(lambda acc, i: acc + 1, 0, reduce=reduce)
