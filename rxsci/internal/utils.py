

class NotSet:
    """Sentinel value."""

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return 'NotSet'
