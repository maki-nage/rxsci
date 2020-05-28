import math
import rx.operators as ops
import rxsci as rs


def stddev(key_mapper=lambda i: i, reduce=False):
    '''Computes the standard deviation.

    This is an approximation of the real standard deviation. See
    rxsci.math.variance for more information.

    Use this operator instead of rxsci.formal.stddev when there are more than
    1000 items in the source observable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the variance.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the variance of the source items. If the number
        of items emitted in the source observable is less than 2, and reduce
        is set, then emits None on completion.
    '''
    def _stddev(source):

        return source.pipe(
            rs.math.variance(key_mapper, reduce=reduce),
            ops.map(lambda i: math.sqrt(i) if i is not None else None),
        )

    return _stddev
