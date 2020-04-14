"""Errors and Warnings."""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""


class DownloadError(RuntimeError):
    """Raised when a download goes wrong."""
