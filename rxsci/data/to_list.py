from collections import deque
import rx.operators as ops
import rx
import rxsci as rs


def to_list_mux():
    def push_to_list(acc, i):
        acc.append(i)
        return acc

    return rs.ops.scan(push_to_list, seed=list, reduce=True)


def to_list():
    ''' flattens list items to a list and publish them when the source
    observable completes.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: to_list

        -1-2-3-4-|
        [     to_list()    ]
        ---------1,2,3,4|

    Returns:
        An observable emitting a single list item then the source observable
        completes.
    '''
    def _to_list(source):
        if isinstance(source, rs.MuxObservable):
            return to_list_mux()(source)
        else:
            return ops.to_list()(source)

    return _to_list
