import rxsci as rs
import rx.operators as ops


def on_probe_state_topology(cbk):
    def _on(source):
        def on_subscribe(observer, scheduler):
            def _on_next(i):
                if type(i) is rs.state.ProbeStateTopology:
                    cbk(i)

                observer.on_next(i)

            return source.subscribe(
                on_next=_on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _on
