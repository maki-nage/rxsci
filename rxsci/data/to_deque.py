from collections import deque
import rx


def to_deque(extend=False):
    ''' flattens list items to a deque fifo and publish them when the source
    observable completes.

    This buffers and emits the items of the source observable as is. Items on
    the deque are pop as they are emitted. This is useful when working on
    batch data since it allows to dereference items as they are processed.

    Args:
        extend: [Optional] When set to true, the deque is extended for each
            item received. The default behavior is to append items to the deque.
    '''
    def _to_deque(source):
        def on_subscribe(observer, scheduler):
            acc = deque()

            def on_next(i):
                nonlocal acc
                if extend is True:
                    acc.extend(i)
                else:
                    acc.append(i)

            def on_completed():
                print("to_deque now flushing")
                try:
                    while True:
                        observer.on_next(acc.popleft())

                except IndexError:
                    pass
                observer.on_completed()

            source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )

        return rx.create(on_subscribe)

    return _to_deque
