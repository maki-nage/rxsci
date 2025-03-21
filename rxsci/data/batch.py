import rx
import rxsci as rs


def batch(batch_size):
    '''Batch source items into lists

    On completion, the pending items are emitted as a partial batch.

    .. marble::
        :alt: batch

        -0-1-2--1-3-4---2-6--|
        [      batch(3)      ]
        -----0,1,2--1,3,4-2,6|

    Args:
        batch_size: The size of each batch.

    Returns:
        An observable emiting the source items batched in groups of batch_size.
    '''
    def _batch(acc, i):
        if acc[1] is True:
            return ([i], False)
        else:
            b = acc[0]
            b.append(i)
            if len(b) == batch_size:            
                return (b, True)
        
            return (b, False)
    
    def _terminate(acc): return (acc[0], True)

    return rx.pipe(
        rs.ops.scan(_batch, seed=([], False), terminator=_terminate),
        rs.ops.filter(lambda i: i[1] == True),
        rs.ops.map(lambda i: i[0]),
    )
