from collections import deque
import rx
import rx.operators as ops
import rxsci as rs


def sort(key=lambda i: i):
    '''sort items according to key

    Items are sorted in ascending order.

    Impementation note: This operator caches all the items of the source
    observable before sorting them. It can be used ONLY on BATCH source, and
    consumes a lot of memory.

    Args:
        key: [Optional] function used to extract the sorting key on each item.

    Returns:
        An observable emitting the sorted items of the source observable.
    '''
    def _sort(source):
        return source.pipe(
            rs.data.to_list(),
            rs.ops.map(lambda i: sorted(i, key=key)),
            rs.data.to_deque(extend=True),
        )

    return _sort
