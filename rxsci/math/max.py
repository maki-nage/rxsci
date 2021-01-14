import rx
import rxsci as rs


def max(key_mapper=lambda i: i, reduce=False):
    '''Returns the maximum value emitted in the source observable.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: max

        -0-1-4-2-3-8-2-6---|
        [       max()      ]
        -0-1-4-----8-------|

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the max.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the max of source items.
    '''
    def accumulate(acc, i):
        i = key_mapper(i)

        if acc is None or i > acc:
            acc = i

        return acc

    return rs.ops.scan(accumulate, None, reduce=reduce)
