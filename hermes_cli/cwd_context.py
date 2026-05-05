"""Context-local working-directory helpers.

Hermes historically passed the active terminal/file-tool cwd through the
process-global ``TERMINAL_CWD`` environment variable. That is fine for a single
agent, but cron can run jobs concurrently. Per-job cron workdirs need the same
semantics without letting sibling jobs clobber each other's cwd.
"""

from __future__ import annotations

import contextvars
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

_TERMINAL_CWD: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "hermes_terminal_cwd",
    default=None,
)


def get_terminal_cwd(default: str | None = None) -> str | None:
    """Return the context-local terminal cwd, falling back to ``TERMINAL_CWD``.

    The fallback preserves existing CLI/gateway behavior for code paths that
    still configure cwd globally. Cron workdir jobs set the ContextVar instead,
    which makes concurrent jobs safe as long as callers use this helper instead
    of reading ``os.environ['TERMINAL_CWD']`` directly.
    """

    value = _TERMINAL_CWD.get()
    if value:
        return value
    return os.getenv("TERMINAL_CWD", default)


def set_terminal_cwd(path: str | os.PathLike[str] | None) -> contextvars.Token:
    """Set the context-local terminal cwd and return the reset token."""

    if path is None:
        return _TERMINAL_CWD.set(None)
    return _TERMINAL_CWD.set(str(Path(path).expanduser()))


def reset_terminal_cwd(token: contextvars.Token) -> None:
    """Restore the previous context-local terminal cwd."""

    _TERMINAL_CWD.reset(token)


@contextmanager
def terminal_cwd(path: str | os.PathLike[str] | None) -> Iterator[None]:
    """Temporarily set the context-local terminal cwd."""

    token = set_terminal_cwd(path)
    try:
        yield
    finally:
        reset_terminal_cwd(token)
