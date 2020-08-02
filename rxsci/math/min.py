import rx
import rxsci.operators as rsops


def min(key_mapper=lambda i: i, reduce=False):
    '''Returns the minimum value emitted in the source observable.

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

    return rsops.scan(accumulate, None, reduce=reduce)
