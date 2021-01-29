import rxsci as rs


def ignore():
    """Ignores errors emitted on a Mux Observable

    Mux errors emitted on the source observable are silently ignored.

    The source must be a MuxObservable.

    .. marble::
        :alt: ignore

        -+-----------------|
         +-1--X--2--3--X--4|
        [     ignore()     ]
        ---1-----2--3-----4|


    Errors at the Observable layer are still propagated downstream and stop the
    pipeline:

    .. marble::
        :alt: ignore_2

        -+----------------*
         +-1--X--2--3--X->
        [     ignore()     ]
        ---1-----2--3-----*


    Examples:
        >>> data = rx.from_([1, 2, 0, 4]).pipe(
        >>>     rs.ops.multiplex(rx.pipe(
        >>>         rs.ops.map(lambda i: 1 / i),
        >>>         rs.error.ignore(),
        >>>     ))
        >>> )

    Returns:
        A MuxObservable emitting all items of the source observable and
        dropping any incoming Mux Error.
    """
    def _ignore(source):
        def on_subscribe(observer, scheduler):
            def _on_next(i):
                if type(i) is not rs.OnErrorMux:
                    observer.on_next(i)

            return source.subscribe(
                on_next=_on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _ignore
