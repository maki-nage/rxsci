import rxsci as rs


def identity():
    """emits an Observable identical to the source Observable.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: identity

        ---1---2---3---4--->
        [     identity     ]
        ---1---2---3---4--->

    Returns:
        An observable emitting the items of the source observable.
    """
    return rs.ops.map(lambda i: i)
