import rx
import rxsci as rs


def variance(key_mapper=lambda i: i, reduce=False):
    '''Computes the variance of the items emitted in the source observable.

    This is an approximation of the real variance. The implementation is based
    on the following article: `standard_deviation <https://www.johndcook.com/blog/standard_deviation/>`_

    Use this operator instead of exact_variance when there are more than 1000
    items in the distribution.

    The source can be an Observable or a MuxObservable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the variance.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the variance of the source items.
    '''
    # state is (m, s, k)
    def accumulate(acc, i):
        m = acc[0]
        s = acc[1]
        k = acc[2] + 1
        i = key_mapper(i)

        if m is None:
            m = i
        else:
            m1 = m
            m = m + (i - m) / k
            s = s + (i - m1)*(i - m)

        return (m, s, k)

    return rx.pipe(
        rs.ops.scan(accumulate, (None, 0, 0), reduce=reduce),
        rs.ops.map(lambda acc: 0.0 if acc[2] < 2 else acc[1] / (acc[2]-1)),
    )
