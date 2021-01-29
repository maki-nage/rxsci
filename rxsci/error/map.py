import rxsci as rs


def map(mapper):
    """maps errors emitted on a Mux Observable and route the result on the item path.

    The source must be a MuxObservable.

    .. marble::
        :alt: map

        -+-----------------|
         +-1--X--2--3--X--4|
        [      map(0)      ]
        ---1--0--2--3--0--4|


    Errors at the Observable layer are still propagated downstream and stop the
    pipeline:

    .. marble::
        :alt: map_2

        -+----------------*
         +-1--X--2--3--X->
        [      map(0)      ]
        ---1--0--2--3--0--*


    Examples:
        >>> data = rx.from_([1, 2, 0, 4]).pipe(
        >>>     rs.ops.multiplex(rx.pipe(
        >>>         rs.ops.map(lambda i: 1 / i),
        >>>         rs.error.map(lambda e: 0),
        >>>     ))
        >>> )

    Returns:
        A MuxObservable emitting all items of the source observable and
        mapping incoming Mux Errors to items.
    """
    def _map(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnErrorMux:
                    try:
                        print("error")
                        ii = mapper(i.error)
                        observer.on_next(rs.OnNextMux(
                            key=i.key,
                            item=ii,
                            store=i.store,
                        ))
                    except Exception as e:
                        observer.on_error(e)
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _map
