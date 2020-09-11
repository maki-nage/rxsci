import rxsci as rs


def pad_start_mux(size, value):
    def _pad_start_mux(source):
        def on_subscribe(observer, scheduler):
            state = rs.mux.MuxState(bool)

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    if not state.is_set(i.key):
                        state.set(i.key, True)
                        v = value if value is not None else i.item
                        for _ in range(size):
                            observer.on_next(rs.OnNextMux(i.key, v))
                    observer.on_next(i)
                elif type(i) is rs.OnCreateMux:
                    state.add_key(i.key)
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux or type(i) is rs.OnErrorMux:
                    observer.on_next(i)
                    state.del_key(i.key)
                else:
                    observer.on_error(TypeError("pad_start: unknow item type: {}".format(type(i))))

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )

        return rs.MuxObservable(on_subscribe)
    return _pad_start_mux


def pad_start(size, value=None):
    """Prepends some items to an Observable

    Prepends a unique value several times on the source observable.

    .. marble::
        :alt: pad_start

        --1-----2--3--4----|
        [  pad_start(2,0)  ]
        --0-0-1-2--3--4----|

    Args:
        size: The number of items to prepend on the source observable
        value: [Optional] The value of each prepended items. If no value it
                set, then the value of the first item is used.

    Source:
        A MuxObservable

    Returns:
        The source observable with size items prepended.

    """
    if size < 0:
        raise ValueError("pad_start: size must be positive")

    def _pad_start(source):
        if isinstance(source, rs.MuxObservable):
            return pad_start_mux(size, value)(source)
        else:
            raise NotImplementedError

    return _pad_start


def pad_end_mux(size, value):
    def _pad_end_mux(source):
        def on_subscribe(observer, scheduler):
            state = rs.mux.MuxState()

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    state.set(i.key, i.item)
                    observer.on_next(i)
                elif type(i) is rs.OnCreateMux:
                    state.add_key(i.key)
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    if state.is_set(i.key):
                        v = value if value is not None else state.get(i.key)
                        for _ in range(size):
                            observer.on_next(rs.OnNextMux(i.key, v))
                    observer.on_next(i)
                    state.del_key(i.key)
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(i)
                    state.del_key(i.key)
                else:
                    observer.on_error(TypeError("pad_end: unknow item type: {}".format(type(i))))

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )

        return rs.MuxObservable(on_subscribe)
    return _pad_end_mux


def pad_end(size, value=None):
    """Appends some items to an Observable

    Appends a unique value several times to the source observable.

    .. marble::
        :alt: pad_end

        --1--2--3--4-|
        [  pad_end(2,0)  ]
        --1--2--3--4-0-0|

    Args:
        size: The number of items to append to the source observable
        value: [Optional] The value of each appended items. If no value it
                set, then the value of the last item is used.

    Source:
        A MuxObservable

    Returns:
        The source observable with size items appended.

    """
    if size < 0:
        raise ValueError("pad_end: size must be positive")

    def _pad_end(source):
        if isinstance(source, rs.MuxObservable):
            return pad_end_mux(size, value)(source)
        else:
            raise NotImplementedError

    return _pad_end
