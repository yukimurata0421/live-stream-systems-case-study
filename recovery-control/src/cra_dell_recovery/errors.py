class RecoveryControlError(Exception):
    """Base class for fail-closed protocol and ledger errors."""


class ProtocolValidationError(RecoveryControlError):
    pass


class ProtocolVersionError(ProtocolValidationError):
    pass


class SignatureValidationError(ProtocolValidationError):
    pass


class UnknownKeyError(SignatureValidationError):
    pass


class LedgerUnavailable(RecoveryControlError):
    pass


class AuthorityDeadlineExceeded(RecoveryControlError):
    """A critical DB operation outlived the authority-validity budget."""


class CommandBlocked(RecoveryControlError):
    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code
