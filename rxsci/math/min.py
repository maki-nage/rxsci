import rx
import rxsci as rs


def min(key_mapper=lambda i: i, reduce=False):
    '''Returns the minimum value emitted in the source observable.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: min

        -9-7-8-2-3-8-2-6---|
        [       min()      ]
        -9-7---2-----------|

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the min.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the min of source items.
    '''
    def accumulate(acc, i):
        i = key_mapper(i)

        if acc is None or i < acc:
            acc = i

        return acc

    return rs.ops.scan(accumulate, None, reduce=reduce)
