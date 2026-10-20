"""Configuration from a non-secret TOML file and environment variables."""
import os
from pathlib import Path
import tomllib
from dataclasses import dataclass, field


class InvalidGroupId(ValueError):
    """A numeric group ID cannot be represented by Telegram or SQLite."""


@dataclass(frozen=True)
class LoggingConfig:
    output: str = "stdout"
    level: str = "INFO"
    file: Path = Path("logs/msgretbot.log")


@dataclass(frozen=True)
class Config:
    mode: str
    groups: tuple[int, ...]
    database: Path
    session: Path
    api_id: int
    api_hash: str = field(repr=False)
    bot_token: str | None = field(default=None, repr=False)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, path: Path) -> "Config":
        with path.open("rb") as stream:
            data = tomllib.load(stream)
        mode = data.get("mode", "hybrid")
        if mode not in ("hybrid", "userbot"):
            raise ValueError("mode must be hybrid or userbot")
        groups = data.get("groups", [])
        if not groups or any(type(g) is not int or g >= 0 for g in groups):
            raise ValueError("groups must contain negative numeric group IDs")
        if len(groups) != len(set(groups)):
            raise ValueError("groups must not contain duplicates")
        if any(g < -(2**63) for g in groups):
            raise InvalidGroupId("Group ID exceeds signed 64-bit range; use the actual Telegram "
                             "group ID (supergroup IDs have the form -100...), not a concatenated ID")
        api_id = int(os.environ["TELEGRAM_API_ID"])
        api_hash = os.environ["TELEGRAM_API_HASH"]
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if api_id <= 0 or not api_hash or (mode == "hybrid" and not token):
            raise ValueError("Missing Telegram credentials")
        root = Path.cwd().resolve()
        def local_path(value: str) -> Path:
            result = (root / value).resolve()
            if not result.is_relative_to(root):
                raise ValueError("Data paths must remain inside the workspace")
            return result
        logging_data = data.get("logging", {})
        if not isinstance(logging_data, dict):
            raise ValueError("logging must be a TOML table")
        output = logging_data.get("output", "stdout")
        level = logging_data.get("level", "INFO")
        if output not in ("stdout", "file"):
            raise ValueError("logging.output must be stdout or file")
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError("Invalid logging.level")
        logging_config = LoggingConfig(output, level, local_path(logging_data.get("file", "logs/msgretbot.log")))
        return cls(mode, tuple(groups), local_path(data.get("database", "data/messages.sqlite3")),
                   local_path(data.get("session", "data/observer.session")), api_id, api_hash, token,
                   logging_config)
