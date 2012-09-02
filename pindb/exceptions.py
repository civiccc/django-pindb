from __future__ import absolute_import

class PinDbException(Exception):
    pass

class PinDbConfigError(PinDbException, ValueError):
    pass


class UnpinnedWriteException(PinDbException):
    pass


