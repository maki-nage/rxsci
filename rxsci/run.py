import threading
import rx
from rx.scheduler import CurrentThreadScheduler


def run(pipe):
    exception = None
    latch = threading.Event()
    has_result = False
    result = None
    done = False

    def on_next(value):
        nonlocal result, has_result
        result = value
        has_result = True

    def on_error(error):
        nonlocal exception, done

        exception = error
        done = True
        latch.set()

    def on_completed() -> None:
        nonlocal done
        done = True
        latch.set()

    pipe.subscribe(on_next, on_error, on_completed, scheduler=CurrentThreadScheduler())

    while not done:
        latch.wait()

    if exception:
        raise exception

    return result
