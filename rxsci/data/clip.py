import rx.operators as ops


def clip(lower_bound=None, higher_bound=None):
    '''clip values between lower_bound and higher_bound

    Raises:
        ValueError if no bound is provided or lower_bound is bigger than
            higher_bound
    '''
    if lower_bound is not None and higher_bound is not None \
            and lower_bound > higher_bound:
        raise ValueError("clip: higher_bound must be bigger than lower_bound")

    if lower_bound is None and higher_bound is None:
        def __clip(i): return i
    elif lower_bound is not None and higher_bound is not None:
        def __clip(i): return max(min(i, higher_bound), lower_bound)
    elif lower_bound is None:
        def __clip(i): return min(i, higher_bound)
    elif higher_bound is None:
        def __clip(i): return max(i, lower_bound)

    def _clip(source):
        return source.pipe(
            ops.map(__clip),
        )

    return _clip
