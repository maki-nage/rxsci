import rxsci as rs
import rx.operators as ops


def do_action_mux(on_next=None, on_error=None, on_completed=None, on_create=None):
    def _do_action_mux(source):
        def on_subscribe(observer, scheduler):
            def _on_next(i):
                if type(i) is rs.OnNextMux:
                    if on_next is not None:
                        on_next(i.item)
                elif type(i) is rs.OnErrorMux:
                    if on_error is not None:
                        on_error(i.error)
                elif type(i) is rs.OnCompletedMux:
                    if on_completed is not None:
                        on_completed(i.key)
                elif type(i) is rs.OnCreateMux:
                    if on_create is not None:
                        on_create(i.key)

                observer.on_next(i)

            def _on_error(e):
                if on_error is not None:
                    on_error(e)
                observer.on_error(e)

            def _on_completed():
                if on_completed is not None:
                    on_completed(None)
                observer.on_completed()

            return source.subscribe(
                on_next=_on_next,
                on_completed=_on_completed,
                on_error=_on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _do_action_mux


def do_action(on_next=None, on_error=None, on_completed=None, on_create=None):
    """Executes an function on reception of selected events

    Source:
        An Observable or a MuxObservable

    Args:
        on_next: [Optional] function to execute on item reception
        on_completed: [Optional] function to execute on completion
        on_error: [Optional] function to execute on error

    Returns:
        An observable identical to the source observable.
    """
    def _do_action(source):
        if isinstance(source, rs.MuxObservable):
            return do_action_mux(on_next, on_error, on_completed, on_create)(source)
        else:
            return ops.do_action(on_next, on_error, on_completed)(source)

    return _do_action
