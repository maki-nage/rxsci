from array import array
import rx
import rxsci as rs


def to_array(typecode):
    ''' flattens list items to an array and publishes them when the source
    observable completes.

    The source can be an Observable or a MuxObservable.

    Args:
        typecode: The type of data to use in the array
    '''
    def _append(acc, i):
        acc.append(i)
        return acc

    return rx.pipe(
        rs.ops.scan(_append, seed=lambda: array(typecode), reduce=True)
    )
