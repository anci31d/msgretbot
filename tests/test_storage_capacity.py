"""Exercise real SQLite files, journaling, eviction and WAL migration."""
import sqlite3
import pytest
from msgretbot.models import Message
from msgretbot.storage import Store, DatabaseCapacityError, DATABASE_BYTES
from conftest import NOW


def size(path):
    return sum(candidate.stat().st_size for candidate in
               (path, path.with_name(path.name + "-journal"),
                path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm"))
               if candidate.exists())


def large_message(mid, text="x" * 8192):
    return Message(1, -10, mid, 20, "Alice", "alice", NOW-1000+mid, text)


async def test_capacity_evicts_oldest_and_preserves_latest(tmp_path, caplog):
    path = tmp_path / "bounded.sqlite3"
    budget = 512 * 1024
    store = await Store.open(path, max_bytes=budget)
    samples = []
    await store.db.set_trace_callback(lambda statement: samples.append(size(path)))
    try:
        for mid in range(1, 101):
            assert await store.save(large_message(mid), NOW)
            assert size(path) <= budget
        retained = await store.active(1, -10, NOW)
        assert retained[-1].id == 100
        assert retained[0].id > 1
        assert [item.id for item in retained] == list(range(retained[0].id, 101))
        assert "Database capacity eviction" in caplog.text
        assert max(samples) <= budget
        assert (await (await store.db.execute("PRAGMA integrity_check")).fetchone())[0] == "ok"
        assert (await (await store.db.execute("PRAGMA max_page_count")).fetchone())[0] == store.page_limit
    finally:
        await store.close()
    assert size(path) <= budget


async def test_queue_payload_growth_obeys_budget_and_cascades(tmp_path):
    path = tmp_path / "bounded.sqlite3"
    budget = 768 * 1024
    store = await Store.open(path, max_bytes=budget)
    try:
        for mid in range(1, 65):
            message = large_message(mid, text="😀" * 1024)
            await store.save(message, NOW)
            await store.deleted(message, NOW)
            await store.prepare(message, ["😀" * 2048])
            assert size(path) <= budget
        rows = await (await store.db.execute("SELECT id FROM messages ORDER BY id")).fetchall()
        assert rows[0][0] > 1
        assert rows[-1][0] == 64
        assert (await (await store.db.execute("PRAGMA foreign_key_check")).fetchall()) == []
        count = (await (await store.db.execute("SELECT count(*) FROM deliveries")).fetchone())[0]
        assert count == len(rows)
    finally:
        await store.close()


async def test_too_large_message_does_not_destroy_history(tmp_path):
    path = tmp_path / "bounded.sqlite3"
    budget = 512 * 1024
    store = await Store.open(path, max_bytes=budget)
    try:
        assert await store.save(large_message(1, "keep me"), NOW)
        assert not await store.save(large_message(2, "x" * budget), NOW)
        assert [m.id for m in await store.active(1, -10, NOW)] == [1]
        assert size(path) <= budget
    finally:
        await store.close()


async def test_existing_wal_database_is_migrated_without_losing_queue(tmp_path):
    path = tmp_path / "old.sqlite3"
    old = await Store.open(path)
    original = large_message(1, "preserve me")
    await old.save(original, NOW)
    await old.deleted(original, NOW)
    await old.prepare(original, ["preserve this pending alert"])
    await old.close()
    # Emulate the previously deployed version before opening the new store.
    legacy = sqlite3.connect(path)
    assert legacy.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    legacy.execute("UPDATE messages SET name='Updated author'")
    legacy.commit()
    legacy.close()
    store = await Store.open(path)
    try:
        assert (await (await store.db.execute("PRAGMA journal_mode")).fetchone())[0] == "truncate"
        row = await store.due(1, NOW, (-10,))
        assert row["name"] == "Updated author"
        assert "preserve this pending alert" in row["parts"]
        assert not path.with_name(path.name + "-wal").exists()
        assert size(path) <= DATABASE_BYTES
    finally:
        await store.close()


async def test_reopen_preserves_evicted_history_and_limit(tmp_path):
    path = tmp_path / "bounded.sqlite3"
    budget = 512 * 1024
    store = await Store.open(path, max_bytes=budget)
    for mid in range(1, 41):
        await store.save(large_message(mid), NOW)
    before = [m.id for m in await store.active(1, -10, NOW)]
    await store.close()
    store = await Store.open(path, max_bytes=budget)
    try:
        assert [m.id for m in await store.active(1, -10, NOW)] == before
        assert await store.save(large_message(41), NOW)
        assert size(path) <= budget
    finally:
        await store.close()


async def test_existing_oversized_database_is_preserved_and_rejected(tmp_path):
    path = tmp_path / "large.sqlite3"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE old_data(value BLOB)")
    db.execute("INSERT INTO old_data VALUES(zeroblob(400000))")
    db.commit()
    db.close()
    before = path.read_bytes()
    with pytest.raises(DatabaseCapacityError):
        await Store.open(path, max_bytes=512 * 1024)
    assert path.read_bytes() == before


async def test_default_fifty_mib_budget_with_live_journal(tmp_path):
    path = tmp_path / "default.sqlite3"
    store = await Store.open(path)
    samples, journals = [], []
    def observe(statement):
        samples.append(size(path))
        journal = path.with_name(path.name + "-journal")
        journals.append(journal.stat().st_size if journal.exists() else 0)
    await store.db.set_trace_callback(observe)
    try:
        for mid in range(1, 17):
            assert await store.save(large_message(mid, "x" * (2 * 1024 * 1024)), NOW)
        retained = await store.active(1, -10, NOW)
        assert retained[0].id > 1
        assert retained[-1].id == 16
        assert max(samples) <= 50 * 1024 * 1024
        assert max(journals) > 0
        assert store.page_limit * store.page_size <= (DATABASE_BYTES - 1024 * 1024) // 2
        assert (await (await store.db.execute("PRAGMA cache_spill")).fetchone())[0] == 0
    finally:
        await store.close()


async def test_sqlite_full_rolls_back_evicts_and_retries(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    path = tmp_path / "full.sqlite3"
    budget = 512 * 1024
    store = await Store.open(path, max_bytes=budget)
    # Bypass only the proactive reservation to exercise SQLite's real hard limit.
    monkeypatch.setattr(store, "_make_room", AsyncMock())
    evict = AsyncMock(wraps=store._evict_oldest)
    monkeypatch.setattr(store, "_evict_oldest", evict)
    try:
        for mid in range(1, 81):
            assert await store.save(large_message(mid), NOW)
            assert size(path) <= budget
        assert evict.await_count > 0
        assert (await store.active(1, -10, NOW))[-1].id == 80
        assert (await (await store.db.execute("PRAGMA integrity_check")).fetchone())[0] == "ok"
    finally:
        await store.close()
