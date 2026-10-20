import logging
import stat
import pytest
from msgretbot.config import LoggingConfig
from msgretbot.logging_setup import configure_logging, FILE_BYTES


@pytest.fixture(autouse=True)
def restore_loggers():
    names = ("msgretbot", "telethon", "aiogram", "aiosqlite")
    state = {}
    for name in names:
        logger = logging.getLogger(name)
        state[name] = (logger.handlers[:], logger.level, logger.propagate, logger.disabled)
        logger.handlers = []
    yield
    for name in names:
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            handler.close()
        logger.handlers, logger.level, logger.propagate, logger.disabled = state[name]


def test_stdout_includes_debug_and_uses_stdout(capsys):
    configure_logging(LoggingConfig(level="DEBUG"))
    logging.getLogger("msgretbot.service").debug("Context selected chat=-10")
    logging.getLogger("aiosqlite").debug("secret SQL parameters")
    captured = capsys.readouterr()
    assert "DEBUG msgretbot.service: Context selected chat=-10" in captured.out
    assert "secret SQL" not in captured.out + captured.err
    assert captured.err == ""


def test_file_rotation_total_byte_budget(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    # Exercise multiple rotations cheaply, including UTF-8 multibyte characters.
    monkeypatch.setattr("msgretbot.logging_setup.FILE_BYTES", 512)
    path = tmp_path / "logs/test.log"
    handler = configure_logging(LoggingConfig("file", "INFO", path))
    for index in range(100):
        logging.getLogger("msgretbot").info("event=%s %s", index, "😀" * 30)
    handler.flush()
    files = sorted(path.parent.iterdir())
    assert len(files) == 2
    assert all(file.stat().st_size <= 512 for file in files)
    assert sum(file.stat().st_size for file in files) <= 1024
    assert "event=99" in path.read_text()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert capsys.readouterr().out == ""
    assert FILE_BYTES * 2 == 10 * 1024 * 1024


def test_exception_details_not_written(capsys):
    configure_logging(LoggingConfig())
    try:
        raise RuntimeError("DO-NOT-LOG-SECRET")
    except RuntimeError:
        logging.getLogger("msgretbot").exception("Worker failed")
    captured = capsys.readouterr().out
    assert "error_type=RuntimeError" in captured
    assert "DO-NOT-LOG-SECRET" not in captured
    assert "Traceback" not in captured


def test_file_outside_workspace_rejected(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    root.mkdir()
    monkeypatch.chdir(root)
    with pytest.raises(ValueError):
        configure_logging(LoggingConfig("file", "INFO", tmp_path / "outside.log"))
