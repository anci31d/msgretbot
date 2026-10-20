"""Offline smoke check using synthetic data only; also usable inside Docker."""
import asyncio
from pathlib import Path
import sys
import time
from msgretbot.models import Message, Presence, TTL
from msgretbot.service import Recovery
from msgretbot.storage import Store


class Reader:
    async def presence(self, chat, ids):
        return {mid: Presence.PRESENT for mid in ids}


class Sender:
    def __init__(self):
        self.sent = []

    async def send(self, chat, html, delivery_key, reply_to=None):
        self.sent.append((chat, html, delivery_key, reply_to))
        return 1000


async def main():
    now = time.time()
    path = Path("data/docker-smoke.sqlite3")
    store = await Store.open(path)
    sender = Sender()
    service = Recovery(store, Reader(), sender, 1, (-10,), 99, clock=lambda: now)
    try:
        for mid in range(1, 5):
            await service.record(Message(1, -10, mid, 20, "Synthetic", None, now-10+mid, "test"))
        await service.deletion([4], -10)
        assert await service.deliver_once()
        assert len(sender.sent) == 1
        assert sender.sent[0][3] == 3
        assert "3." in sender.sent[0][1]
        assert (await (await store.db.execute("PRAGMA journal_mode")).fetchone())[0] == "truncate"
        await store.cleanup(now + TTL + 1)
        assert await store.active(1, -10, now + TTL + 1) == []
        print("Docker smoke: SQLite + queue + context + reply + TTL OK; no network used")
    finally:
        await store.close()


async def lifecycle():
    from msgretbot import app
    async def holding(config, command):
        store = await Store.open(Path("data/docker-lifecycle.sqlite3"))
        try:
            print("Lifecycle smoke: ready", flush=True)
            await asyncio.Future()
        finally:
            await store.close()
            print("Lifecycle smoke: database closed", flush=True)
    app.run = holding
    await app.serve(None, "run")


if __name__ == "__main__":
    asyncio.run(lifecycle() if "--lifecycle" in sys.argv else main())
