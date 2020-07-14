class InsufficientBalanceException(Exception):
    pass


class NoPathFoundException(Exception):
    pass


class StatelessInvalidError(Exception):
    pass


class InconsistentBlockException(Exception):
    pass


class InvalidClaimException(Exception):
    pass


class InconsistentClaimException(Exception):
    pass


class InconsistentStateHashException(Exception):
    pass


class InvalidMintException(Exception):
    pass


class UnknownMinterException(InvalidMintException):
    pass


class InvalidMintRangeException(InvalidMintException):
    pass


class UnboundedMintException(InvalidMintException):
    pass


class InvalidWitnessTransactionException(Exception):
    pass
