from collections import deque
import rx
import rx.operators as ops
import rxsci as rs


def sort(key=lambda i: i, reverse=False):
    '''sort items according to key

    Items are sorted in ascending order by default. When reverse is set to
    True, they are sorted by descending order.

    Impementation note: This operator caches all the items of the source
    observable before sorting them. Si, it can be used only on a batch source,
    and can lead to high memory usage.

    The source must be an Observable.

    Args:
        key: [Optional] function used to extract the sorting key on each item.
        reverse: [Optional] Set to True for descendig sorting.


    Returns:
        An observable emitting the sorted items of the source observable.
    '''
    def _sort(source):
        return source.pipe(
            rs.data.to_list(),
            rs.ops.map(lambda i: sorted(i, key=key, reverse=reverse)),
            rs.data.to_deque(extend=True),
        )

    return _sort
