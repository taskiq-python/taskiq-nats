class BaseTaskiqNATSError(Exception):
    """Base error for all possible exception in the lib."""


class ResultIsMissingError(BaseTaskiqNATSError):
    """Error if cannot retrieve result from NATS Object Store."""
