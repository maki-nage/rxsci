

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
