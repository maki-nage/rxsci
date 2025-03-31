import rx
import rxsci as rs


def create_error_router():
    """Creates a route for mux errors

    Errors on MuxObservables are not fatal. This error handler allows to route
    them to a dedicated Observable. This operator allows to implement a dead
    letter box observable or other kinds of error monitoring.

    The source must be a MuxObservable.

    .. marble::
        :alt: route_errors

        -+-----------------|
         +-1--X--2--3--X--4|
        [  route_errors()  ]
        ---1-----2--3-----4|
        ------X--------X---|


    Errors at the Observable layer are still propagated downstream and stop the
    pipeline:

    .. marble::
        :alt: route_errors_2

        -+----------------*
         +-1--X--2--3--X->
        [  route_errors()  ]
        ---1-----2--3------*
        ------X--------X---X|


    Examples:
        >>> errors, route_errors = rs.error.create_error_router()
        >>> data = rx.from_([1, 2, 0, 4]).pipe(
        >>>     rs.ops.multiplex(rx.pipe(
        >>>         rs.ops.map(lambda i: 1 / i),
        >>>         route_errors(),
        >>>     ))
        >>> )

    Returns:
        A tuple of (Observable, function). Observable is the errors
        observable where all mux errors will be routed to. The function is an
        operator to used in a pipeline where errors are be routed to the errors
        observable.
    """
    dead_letter_observer = None

    def on_dead_letter_dispose():
        nonlocal dead_letter_observer
        dead_letter_observer = None

    def on_dead_letter_subscribe(observer, scheduler):
        nonlocal dead_letter_observer
        assert dead_letter_observer is None
        dead_letter_observer = observer
        return rx.disposable.Disposable(on_dead_letter_dispose)

    def _route_to_dead_letter():
        def route_to_dead_letter(source):
            def on_subscribe(observer, scheduler):
                def on_next(i):
                    if type(i) is rs.OnErrorMux and dead_letter_observer is not None:
                        dead_letter_observer.on_next(i.error)
                    else:
                        observer.on_next(i)

                def on_error(e):
                    if dead_letter_observer is not None:
                        dead_letter_observer.on_next(e)
                        dead_letter_observer.on_completed()

                    observer.on_error(e)

                def on_completed():
                    if dead_letter_observer is not None:
                        dead_letter_observer.on_completed()

                    observer.on_completed()

                return source.subscribe(
                    on_next=on_next,
                    on_error=on_error,
                    on_completed=on_completed,
                    scheduler=scheduler,
                )

            return rs.MuxObservable(on_subscribe)
        return route_to_dead_letter

    return rx.create(on_dead_letter_subscribe), _route_to_dead_letter
