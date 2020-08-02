import rx
import rxsci.operators as rsops


def mean(key_mapper=lambda i: i, reduce=False):
    '''Averages the items emitted in the source observable

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
        rsops.scan(accumulate, (0, 0), reduce=reduce),
        rsops.map(lambda acc: acc[0] / acc[1] if acc is not None else None),
    )
