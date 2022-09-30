import rx
from rx.scheduler import CurrentThreadScheduler
from rx.disposable import CompositeDisposable, Disposable

from rxsci.internal.utils import build_tdqm_kwargs

try:
    from tqdm.auto import tqdm
except Exception:
    pass


def from_iterable(iterable, scheduler=None, progress=False):
    """Converts an iterable to an observable.

    Args:
        iterable: A Python iterable
        scheduler: An optional scheduler to schedule the values on.
        progress: a boolean or dict to display a progressbar

    Returns:
        An observable that emits one item per element in the source iterable.
    """

    def subscribe(observer, scheduler_):
        _scheduler = scheduler or scheduler_ or CurrentThreadScheduler.singleton()
        itbl = iterable
        disposed = False

        if progress:
            tqdm_kwargs = build_tdqm_kwargs(progress)
            itbl = tqdm(itbl, **tqdm_kwargs)
        iterator = iter(itbl)

        def action(_, __=None):
            nonlocal disposed

            try:
                while not disposed:
                    value = next(iterator)
                    observer.on_next(value)
            except StopIteration:
                observer.on_completed()
            except Exception as error:
                observer.on_error(error)

        def dispose() -> None:
            nonlocal disposed
            disposed = True

        disp = Disposable(dispose)
        return CompositeDisposable(_scheduler.schedule(action), disp)
    return rx.create(subscribe)