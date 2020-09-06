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
    '''cache items

    Each received items is cached, and the cached item is returned. This
    operator can save memory on graphs that buffer some items, with many
    identical values.

    Args:
        field: [Optional] cache the provided field is set. Otherwise the 
                whole item is cached. When field is set, this field value
                must be accessible via the getattr function.

    Returns:
        An Observable returning the same items than the source observable, but 
        with cached values.
    '''
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
