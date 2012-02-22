class PinDBException(Exception):
    pass

class PinDBConfigError(PinDBException, ValueError):
    pass


class UnpinnedWriteException(PinDBException):
    pass


