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


# Mint exceptions:


class InvalidMintException(Exception):
    pass


class UnknownMinterException(InvalidMintException):
    pass


class InvalidMintRangeException(InvalidMintException):
    pass


class UnboundedMintException(InvalidMintException):
    pass


# Spend exceptions:
class InvalidSpendRangeException(Exception):
    pass


class InvalidWitnessTransactionException(Exception):
    pass
