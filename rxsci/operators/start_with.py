import rxsci as rs
import rx.operators as ops


def start_with_mux(padding):
    def _start_with_mux(source):
        def on_subscribe(observer, scheduler):
            state = rs.mux.MuxState(bool)

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    if not state.is_set(i.key):
                        state.set(i.key, True)
                        for p in padding:
                            observer.on_next(rs.OnNextMux(i.key, p))
                    observer.on_next(i)
                elif type(i) is rs.OnCreateMux:
                    state.add_key(i.key)
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux or type(i) is rs.OnErrorMux:
                    observer.on_next(i)
                    state.del_key(i.key)
                else:
                    observer.on_error(TypeError("start_with: unknow item type: {}".format(type(i))))

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )

        return rs.MuxObservable(on_subscribe)
    return _start_with_mux


def start_with(padding):
    """Prepends some items to an Observable
    """
    def _start_with(source):
        if isinstance(source, rs.MuxObservable):
            return start_with_mux(padding)(source)
        else:
            return ops.start_with(*padding)(source)

    return _start_with
