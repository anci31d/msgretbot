from dataclasses import replace
from types import SimpleNamespace
import pytest
import pytest_asyncio
from msgretbot.models import Message, Presence
from msgretbot.service import Recovery
from msgretbot.storage import Store

NOW = 1_000_000.0


class Reader:
    def __init__(self):
        self.states = {}
        self.requests = []

    async def presence(self, chat, ids):
        self.requests.append((chat, ids))
        return {mid: self.states.get(mid, Presence.PRESENT) for mid in ids}


class Sender:
    def __init__(self):
        self.calls = []
        self.error = None

    async def send(self, chat, html, key):
        if self.error:
            raise self.error
        self.calls.append((chat, html, key))
        return 5000 + len(self.calls)


@pytest.fixture
def message():
    def make(mid=10, **changes):
        return replace(Message(1, -10, mid, 20, "Alice", "alice", NOW-100, "original"), **changes)
    return make


@pytest_asyncio.fixture
async def setup(tmp_path):
    store = await Store.open(tmp_path / "messages.sqlite3")
    reader, sender = Reader(), Sender()
    clock = [NOW]
    service = Recovery(store, reader, sender, 1, (-10, -20), 99, clock=lambda: clock[0])
    yield SimpleNamespace(store=store, reader=reader, sender=sender, service=service, clock=clock,
                          path=tmp_path / "messages.sqlite3")
    await store.close()
