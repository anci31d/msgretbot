"""Application-only stdout or bounded UTF-8 rotating-file logging."""
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import sys
from .config import LoggingConfig

FILE_BYTES = 5 * 1024 * 1024


class CompactFormatter(logging.Formatter):
    def format(self, record):
        # Exception messages/tracebacks may include Telegram requests and credentials.
        record = logging.makeLogRecord(record.__dict__)
        error = record.exc_info[0].__name__ if record.exc_info else None
        record.exc_info = record.exc_text = record.stack_info = None
        result = super().format(record)
        if error:
            result += f" error_type={error}"
        return result[:4000]


class ByteRotatingHandler(RotatingFileHandler):
    """Count UTF-8 bytes rather than characters when enforcing the file budget."""
    def _open(self):
        return open(self.baseFilename, self.mode, encoding="utf-8", errors="backslashreplace",
                    opener=lambda path, flags: os.open(path, flags, 0o600))

    def shouldRollover(self, record):
        if self.stream is None:
            self.stream = self._open()
        self.stream.seek(0, 2)
        size = self.stream.tell()
        encoded = (self.format(record) + self.terminator).encode("utf-8", errors="backslashreplace")
        return size > 0 and size + len(encoded) >= self.maxBytes


def configure_logging(config: LoggingConfig) -> logging.Handler:
    if config.output == "file":
        path = Path(config.file).resolve()
        if not path.is_relative_to(Path.cwd().resolve()):
            raise ValueError("Log file must remain inside the workspace")
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = ByteRotatingHandler(path, maxBytes=FILE_BYTES, backupCount=1, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CompactFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger = logging.getLogger("msgretbot")
    for previous in logger.handlers[:]:
        logger.removeHandler(previous)
        previous.close()
    logger.addHandler(handler)
    logger.setLevel(config.level)
    logger.propagate = False
    # Libraries can log request bodies, credentials or database parameters at DEBUG.
    for name in ("telethon", "aiogram", "aiosqlite"):
        logging.getLogger(name).disabled = True
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)
    logger.info("Logging configured output=%s level=%s", config.output, config.level)
    return handler
