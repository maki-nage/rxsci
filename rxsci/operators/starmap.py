import rx.operators as ops
import rxsci as rs
from .map import map as map


def starmap(mapper):
    """
    Unpacks arguments grouped as tuple items of an observable
    and return an observable of values by invoking
    the mapper function with star applied unpacked items as
    positional arguments.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: starmap

        -----1,2---3,4-----|
        [   starmap(add)   ]
        -----3-----7-------|

    Example:
        >>> rs.ops.starmap(lambda x, y: x + y)

    Args:
        mapper: A transform function to invoke with unpacked elements
            as arguments.

    Returns:
        An observable containing the results of
        invoking the mapper function with unpacked items of the
        source observable.
    """
    return map(lambda i: mapper(*i))
