import sys
from collections import namedtuple
import functools
import rx


CacheState = namedtuple('CacheState', ['field', 'item'])
CacheState.__new__.__defaults__ = (None,)


def _cache_item(intern, field, i):
    if field is not None:
        ii = getattr(i, field)
        cached_field = intern(ii)
        cached_item = i._replace(**{field: cached_field})
    else:
        cached_item = intern(i)
    return cached_item


def obj_intern(state, i):
    try:
        return state[i]
    except KeyError:
        state[i] = i
        return i


def get_intern(i):
    if type(i) is str:
        return sys.intern
    else:
        state = {}
        return functools.partial(obj_intern, state)


def cache(field=None):
    def _cache(source):
        def on_subscribe(observer, scheduler):
            intern = None

            def on_next(i):
                nonlocal intern
                if i is None:
                    observer.on_next(i)
                else:
                    if intern is None:
                        intern = get_intern(i)

                i = _cache_item(intern, field, i)
                observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rx.create(on_subscribe)

    return _cache
