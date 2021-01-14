import rx
import rxsci as rs


def distinct_until_changed(key_mapper=None):
    """Returns an observable sequence that contains only distinct
    contiguous items according to the key_mapper.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: distinct_until_changed

        -0-1-1-2-3-1-2-2-3-|
        [distinct_until_changed()]
        -0-1---2-3-1-2---3-|

    Args:
        key_mapper: [Optional]  A function to compute the comparison
            key for each element.

    Returns:
        An observable emitting only the distinct contiguous items.
    """
    def _distinct(acc, i):
        key = i
        if key_mapper:
            key = key_mapper(i)

        if key != acc[2]:
            return (True, i, key)
        return (False, i, key)

    return rx.pipe(
        rs.ops.scan(_distinct, seed=(False, None, None)),
        rs.ops.filter(lambda i: i[0] is True),
        rs.ops.map(lambda i: i[1]),
    )
