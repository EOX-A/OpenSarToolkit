"""Errors and Warnings."""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""


class DownloadError(RuntimeError):
    """Raised when a download goes wrong."""


class InvalidFileError(RuntimeError):
    """Raised when an output file did not pass the validation test."""
    def __init__(self, message):
        self.message = message
