

class NotSet(object):
    """Sentinel value."""

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return 'NotSet'


class StateNotSet(object):
    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return 'NotSet'

    def value(self):
        return 0


class StateSet(object):
    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return 'Set'

    def value(self):
        return 1


class StateCleared(object):
    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return 'Cleared'

    def value(self):
        return 2


def build_tdqm_kwargs(progress):
    """Generates a kwargs dict for tqdm based on rxsci progress options.

    Args: 
        progress: Boolean or dict

    Returns:
        A dict containing tqdm kwargs
    """
    tqdm_kwargs = {}
    if type(progress) is dict:
        if 'interval' in progress:
            tqdm_kwargs['mininterval'] = progress['interval']
            tqdm_kwargs['maxinterval'] = progress['interval']
        if 'prefix' in progress:
            tqdm_kwargs['desc'] = progress['prefix']
        if 'eol' in progress:
            tqdm_kwargs['bar_format'] = '{l_bar}{bar}{r_bar}' + progress['eol']

    return tqdm_kwargs
