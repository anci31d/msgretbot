"""CLI lifecycle checks without a Telegram connection."""
import fcntl
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from msgretbot.app import initialize_recovery, run
from msgretbot.config import Config


def configuration(tmp_path):
    return Config("userbot", (-10,), tmp_path / "messages.sqlite3",
                  tmp_path / "observer.session", 123, "test-hash")


async def test_run_requires_explicit_login_and_releases_lock(tmp_path, monkeypatch):
    client = SimpleNamespace(connect=AsyncMock(), is_user_authorized=AsyncMock(return_value=False),
                             disconnect=AsyncMock(), start=AsyncMock())
    monkeypatch.setattr("msgretbot.app.TelegramClient", lambda *a, **kw: client)
    previous_umask = os.umask(0o077)
    try:
        with pytest.raises(ValueError, match="login"):
            await run(configuration(tmp_path), "run")
    finally:
        os.umask(previous_umask)
    client.start.assert_not_awaited()
    client.disconnect.assert_awaited_once()
    with (tmp_path / "observer.session.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


async def test_duplicate_session_rejected_before_client_creation(tmp_path, monkeypatch):
    factory = AsyncMock()
    monkeypatch.setattr("msgretbot.app.TelegramClient", factory)
    previous_umask = os.umask(0o077)
    try:
        with (tmp_path / "observer.session.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(ValueError, match="Another process"):
                await run(configuration(tmp_path), "run")
    finally:
        os.umask(previous_umask)
    factory.assert_not_called()


async def test_login_only_authorizes_and_disconnects(tmp_path, monkeypatch, capsys):
    client = SimpleNamespace(start=AsyncMock(), get_me=AsyncMock(return_value=SimpleNamespace(bot=False)),
                             disconnect=AsyncMock())
    monkeypatch.setattr("msgretbot.app.TelegramClient", lambda *a, **kw: client)
    previous_umask = os.umask(0o077)
    try:
        await run(configuration(tmp_path), "login")
    finally:
        os.umask(previous_umask)
    client.start.assert_awaited_once()
    client.disconnect.assert_awaited_once()
    assert "No group messages sent" in capsys.readouterr().out
    assert not (tmp_path / "messages.sqlite3").exists()


async def test_graceful_termination_closes_application_resources(tmp_path, monkeypatch):
    import asyncio
    from msgretbot.app import serve
    ready, closed = asyncio.Event(), asyncio.Event()
    async def running(config, command):
        try:
            ready.set()
            await asyncio.Future()
        finally:
            closed.set()
    monkeypatch.setattr("msgretbot.app.run", running)
    task = asyncio.create_task(serve(configuration(tmp_path), "run"))
    await ready.wait()
    task.cancel()
    await task
    assert closed.is_set()


async def test_reconciliation_polls_every_thirty_seconds_and_skips_disconnect(monkeypatch):
    import asyncio
    from unittest.mock import Mock
    from msgretbot.app import monitor_reconciliation
    client = SimpleNamespace(is_connected=Mock(side_effect=[False, True]))
    service = SimpleNamespace(reconcile=AsyncMock())
    sleep = AsyncMock(side_effect=[None, None, asyncio.CancelledError()])
    monkeypatch.setattr("msgretbot.app.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await monitor_reconciliation(client, service)
    assert [call.args for call in sleep.call_args_list] == [(30,), (30,), (30,)]
    service.reconcile.assert_awaited_once()


async def test_pending_alert_is_delivered_while_bootstrap_is_blocked(setup, message):
    import asyncio

    delivered = asyncio.Event()
    original_send = setup.sender.send

    async def send_and_signal(chat, html, key):
        result = await original_send(chat, html, key)
        delivered.set()
        return result

    setup.sender.send = send_and_signal
    await setup.service.record(message())
    await setup.service.deletion([10], -10)

    class Reader:
        async def bootstrap(self, service):
            await asyncio.wait_for(delivered.wait(), timeout=3)

    queue = asyncio.Queue()

    async def consume():
        while True:
            await queue.get()
            queue.task_done()

    client = SimpleNamespace(catch_up=AsyncMock())
    tasks = []
    try:
        await initialize_recovery(Reader(), setup.service, client, queue, consume, tasks, "userbot")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert len(setup.sender.calls) == 1
