import rx
import rxsci as rs
from rxsci.math.formal import _moment


def variance(key_mapper=lambda i: i, reduce=False):
    ''' Computes the variance of the items emitted in the source observable.

    The implementation is based on the formal definition of the variance.
    This implies that all items are cached in memory to do the computation.
    Use the rxsci.math.variance operator to compute varianc on a large
    observable.

    The source can be an Observable or a MuxObservable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the variance.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting variance of source items.
    '''
    def accumulate(acc, i):
        i = key_mapper(i)
        acc.append(i)
        return acc

    def _variance(acc):
        if len(acc) == 0:
            return 0.0
        else:
            mean = _moment(acc, 0, 1)
            v = _moment(acc, mean, 2)
            acc.clear()
            return v

    return rx.pipe(
        rs.ops.scan(accumulate, [], reduce=reduce),
        rs.ops.map(_variance),
    )
