import pytest
from msgretbot.config import Config


@pytest.fixture
def config_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.setenv("TELEGRAM_API_HASH", "test-hash")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    return tmp_path / "config.toml"


def test_configuration_and_secret_repr(config_path):
    config_path.write_text('mode="hybrid"\ngroups=[-10,-20]\n')
    config = Config.load(config_path)
    assert config.groups == (-10, -20)
    assert config.database.is_relative_to(config_path.parent)
    assert "test-hash" not in repr(config)
    assert "test-token" not in repr(config)


@pytest.mark.parametrize("body", [
    'groups=[]', 'groups=[-10,-10]', 'groups=[true]', 'groups=[10]',
    'groups=[-10]\nmode="invalid"', 'groups=[-10]\nsession="../observer.session"'])
def test_rejects_invalid_configuration(config_path, body):
    config_path.write_text(body)
    with pytest.raises(ValueError):
        Config.load(config_path)


def test_userbot_needs_no_bot_token(config_path, monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN")
    config_path.write_text('mode="userbot"\ngroups=[-10]')
    assert Config.load(config_path).bot_token is None


def test_oversized_group_id_rejected(config_path):
    config_path.write_text('mode="userbot"\ngroups=[-100100123456789012345]')
    with pytest.raises(ValueError, match="64-bit range"):
        Config.load(config_path)


def test_logging_defaults_and_file_configuration(config_path):
    config_path.write_text('groups=[-10]')
    assert Config.load(config_path).logging.output == "stdout"
    config_path.write_text('groups=[-10]\n[logging]\noutput="file"\nlevel="DEBUG"\nfile="logs/test.log"')
    logging = Config.load(config_path).logging
    assert logging.output == "file"
    assert logging.level == "DEBUG"
    assert logging.file == config_path.parent / "logs/test.log"


def test_alert_mode_defaults_to_loud_and_accepts_silent(config_path):
    config_path.write_text('groups=[-10]')
    assert Config.load(config_path).alerts.mode == "loud"
    config_path.write_text('groups=[-10]\n[alerts]\nmode="silent"')
    assert Config.load(config_path).alerts.mode == "silent"


@pytest.mark.parametrize("body", [
    'output="stderr"', 'level="EVERYTHING"', 'file="../test.log"'])
def test_invalid_logging_config(config_path, body):
    config_path.write_text('groups=[-10]\n[logging]\n' + body)
    with pytest.raises(ValueError):
        Config.load(config_path)


def test_invalid_alert_mode(config_path):
    config_path.write_text('groups=[-10]\n[alerts]\nmode="quiet"')
    with pytest.raises(ValueError, match="alerts.mode"):
        Config.load(config_path)
