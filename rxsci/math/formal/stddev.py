import math
import rx.operators as ops
import rxsci as rs


def stddev(key_mapper=lambda i: i, reduce=False):
    '''Computes standard deviation

    The implementation is based on the formal definition of the standard
    deviation. This implies that all items are cached in memory to do the
    computation. Use the rxsci.math.stddev operator to compute standard
    deviation on a large observable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the standard deviation.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting standard deviation of source items.
    '''
    def _stddev(source):
        return source.pipe(
            rs.math.formal.variance(key_mapper, reduce=reduce),
            ops.map(lambda i: math.sqrt(i) if i is not None else None),
        )

    return _stddev
