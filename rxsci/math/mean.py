import rx
import rxsci as rs


def mean(key_mapper=lambda i: i, reduce=False):
    '''Averages the items emitted in the source observable

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: mean

        -0---1---2---3---4---|
        [        max()       ]
        -0--0.5--1---1.5-2.0-|

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the average.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting items whose value is the sum of source items.
    '''
    def accumulate(acc, i):
        i = key_mapper(i)
        return (acc[0]+i, acc[1]+1)

    return rx.pipe(
        rs.ops.scan(accumulate, (0, 0), reduce=reduce),
        rs.ops.map(lambda acc: acc[0] / acc[1] if acc is not None else None),
    )
