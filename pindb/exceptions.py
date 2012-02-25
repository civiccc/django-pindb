class PinDbException(Exception):
    pass

class PinDbConfigError(PinDbException, ValueError):
    pass


class UnpinnedWriteException(PinDbException):
    pass


