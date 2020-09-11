import rx
import rxsci as rs


def sum(key_mapper=lambda i: i, reduce=False):
    '''Sums the items emitted in the source observable

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the sum.
        reduce: [Optional] Emit an item for each source item when reduce is 
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting items whose value is the sum of source items.
    '''
    def accumulate(acc, i):
        i = key_mapper(i)
        return acc + i

    return rs.ops.scan(accumulate, 0.0, reduce=reduce)
