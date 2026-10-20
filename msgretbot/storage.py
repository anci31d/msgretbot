"""Bounded SQLite persistence and transactional notification queue."""
import asyncio
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
import json
import logging
import math
from pathlib import Path
import sqlite3
import time
import aiosqlite
from .models import Message, TTL

log = logging.getLogger(__name__)
DATABASE_BYTES = 50 * 1024 * 1024

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
 account INTEGER NOT NULL, chat INTEGER NOT NULL, id INTEGER NOT NULL,
 author INTEGER, name TEXT NOT NULL, username TEXT, sent_at REAL NOT NULL,
 text TEXT, kind TEXT NOT NULL, own_alert INTEGER NOT NULL DEFAULT 0,
 deleted INTEGER NOT NULL DEFAULT 0,
 revision_at REAL NOT NULL DEFAULT 0,
 PRIMARY KEY(account, chat, id)
);
CREATE INDEX IF NOT EXISTS message_expiry ON messages(sent_at);
CREATE INDEX IF NOT EXISTS message_lookup ON messages(account, id);
CREATE TABLE IF NOT EXISTS cooldowns (
 account INTEGER PRIMARY KEY, until REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chat_cooldowns (
 account INTEGER NOT NULL, chat INTEGER NOT NULL, until REAL NOT NULL,
 PRIMARY KEY(account, chat)
);
CREATE TABLE IF NOT EXISTS deliveries (
 account INTEGER NOT NULL, chat INTEGER NOT NULL, id INTEGER NOT NULL,
 parts TEXT, next_part INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
 done INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(account, chat, id),
 FOREIGN KEY(account, chat, id) REFERENCES messages(account, chat, id) ON DELETE CASCADE
);
PRAGMA user_version=2;
"""


class DatabaseCapacityError(Exception):
    """An existing database or a single operation cannot fit the disk budget."""


class Store:
    def __init__(self, db: aiosqlite.Connection, page_size: int, page_limit: int):
        self.db = db
        self.page_size = page_size
        self.page_limit = page_limit
        self.lock = asyncio.Lock()
        self.pins = Counter()

    @classmethod
    async def open(cls, path: Path, max_bytes: int = DATABASE_BYTES):
        path.parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(path, isolation_level=None)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA busy_timeout=5000")
            page_size = (await (await db.execute("PRAGMA page_size")).fetchone())[0]
            page_count = (await (await db.execute("PRAGMA page_count")).fetchone())[0]
            # Reserve a second copy of every page for the rollback journal, plus
            # journal headers, page record overhead and sector alignment.
            overhead = min(1024 * 1024, max(64 * 1024, max_bytes // 50))
            page_limit = (max_bytes - overhead) // (2 * page_size)
            if page_limit < 32 or page_count > page_limit:
                log.error("Existing database exceeds safe capacity pages=%s limit=%s; offline compaction required",
                          page_count, page_limit)
                raise DatabaseCapacityError("Database cannot fit the bounded rollback-journal budget")
            await db.execute(f"PRAGMA max_page_count={page_limit}")
            # One in-memory dirty-page set prevents multiple journal headers due
            # to cache spills. No disk-backed sort/temp files are needed.
            await db.execute("PRAGMA cache_spill=OFF")
            await db.execute("PRAGMA temp_store=MEMORY")
            journal = (await (await db.execute("PRAGMA journal_mode=TRUNCATE")).fetchone())[0]
            if journal != "truncate":
                raise DatabaseCapacityError("Cannot enable bounded rollback journal")
            await db.execute("PRAGMA journal_size_limit=0")
            await db.execute("PRAGMA synchronous=FULL")
            await db.execute("PRAGMA foreign_keys=ON")
            row = await (await db.execute("PRAGMA user_version")).fetchone()
            if row[0] > 2:
                raise ValueError("Database schema is newer than this application")
            store = cls(db, page_size, page_limit)
            if row[0] == 1:
                # A full v1 database needs room before adding the pacing table.
                async with store.lock:
                    await store._make_room(32 * 1024)
            await db.executescript(SCHEMA)
            async with store.lock:
                await store._make_room(32 * 1024)
            log.info("Database opened schema=2 journal=TRUNCATE total_budget=%s main_limit=%s",
                     max_bytes, page_limit * page_size)
            return store
        except BaseException:
            await db.close()
            raise

    async def close(self):
        await self.db.close()

    @staticmethod
    def message(row) -> Message:
        return Message(**{key: row[key] for key in Message.__dataclass_fields__})

    @staticmethod
    def key(message: Message):
        return message.account, message.chat, message.id

    def pin(self, key):
        self.pins[key] += 1

    def unpin(self, key):
        self.pins[key] -= 1
        if self.pins[key] <= 0:
            del self.pins[key]

    @contextmanager
    def protect(self, keys):
        keys = tuple(keys)
        self.pins.update(keys)
        try:
            yield
        finally:
            for key in keys:
                self.unpin(key)

    async def _evict_oldest(self, protected=None) -> int:
        restriction, params = "", []
        keys = set(self.pins)
        if protected is not None:
            keys.add(protected)
        if keys:
            restriction = " WHERE NOT (" + " OR ".join(
                "(account=? AND chat=? AND id=?)" for _ in keys) + ")"
            params = [value for key in keys for value in key]
        await self.db.execute("BEGIN IMMEDIATE")
        try:
            removed = await self.db.execute("""DELETE FROM messages WHERE rowid IN
                (SELECT rowid FROM messages""" + restriction +
                " ORDER BY sent_at,account,chat,id LIMIT 16)", params)
            await self.db.commit()
        except BaseException:
            await self.db.rollback()
            raise
        if removed.rowcount:
            log.warning("Database capacity eviction count=%s; oldest messages and related deliveries removed",
                        removed.rowcount)
        return removed.rowcount

    async def _make_room(self, reserve: int, protected=None):
        required = math.ceil(reserve / self.page_size) + 4
        if required > self.page_limit - 16:
            raise DatabaseCapacityError("Operation is larger than the database budget")
        while True:
            pages = (await (await self.db.execute("PRAGMA page_count")).fetchone())[0]
            free = (await (await self.db.execute("PRAGMA freelist_count")).fetchone())[0]
            if self.page_limit - pages + free >= required:
                return
            if not await self._evict_oldest(protected):
                raise DatabaseCapacityError("Operation cannot fit without losing its protected message")

    async def _write(self, operation, protected=None, reserve: int = 32 * 1024):
        async with self.lock:
            await self._make_room(reserve, protected)
            while True:
                await self.db.execute("BEGIN IMMEDIATE")
                try:
                    result = await operation()
                    await self.db.commit()
                    return result
                except BaseException as error:
                    await self.db.rollback()
                    if isinstance(error, sqlite3.DatabaseError) and (
                            getattr(error, "sqlite_errorcode", 0) & 255) == sqlite3.SQLITE_FULL:
                        if await self._evict_oldest(protected):
                            continue
                        raise DatabaseCapacityError("No reclaimable space remains") from None
                    raise

    async def save(self, message: Message, now: float):
        if message.sent_at <= now - TTL:
            log.debug("Expired message skipped chat=%s message=%s", message.chat, message.id)
            return False
        values = asdict(message)
        async def operation():
            await self.db.execute("""
                INSERT INTO messages(account,chat,id,author,name,username,sent_at,text,kind,own_alert,revision_at)
                VALUES(:account,:chat,:id,:author,:name,:username,:sent_at,:text,:kind,:own_alert,:revision_at)
                ON CONFLICT(account,chat,id) DO UPDATE SET
                  author=excluded.author,name=excluded.name,username=excluded.username,
                  text=CASE WHEN excluded.revision_at>=messages.revision_at THEN excluded.text ELSE messages.text END,
                  kind=CASE WHEN excluded.revision_at>=messages.revision_at THEN excluded.kind ELSE messages.kind END,
                  revision_at=MAX(messages.revision_at,excluded.revision_at),
                  own_alert=MAX(messages.own_alert,excluded.own_alert)
                """, values)
        payload_bytes = sum(len(value.encode("utf-8")) for value in values.values() if isinstance(value, str))
        try:
            await self._write(operation, self.key(message), reserve=payload_bytes + 32 * 1024)
        except DatabaseCapacityError:
            log.error("Message exceeds database capacity; not saved chat=%s message=%s", message.chat, message.id)
            return False
        log.info("Message saved account=%s chat=%s message=%s kind=%s own_alert=%s revision=%s",
                 message.account, message.chat, message.id, message.kind,
                 bool(message.own_alert), message.revision_at)
        return True

    async def cleanup(self, now: float):
        async with self.lock:
            removed = await self.db.execute("DELETE FROM messages WHERE sent_at<=?", (now - TTL,))
            await self.db.execute("DELETE FROM cooldowns WHERE until<=?", (now,))
            await self.db.execute("DELETE FROM chat_cooldowns WHERE until<=?", (now,))
            if removed.rowcount:
                log.info("Expired messages purged count=%s; related deliveries removed", removed.rowcount)
            else:
                log.debug("Expiry cleanup completed count=0")

    async def active(self, account: int, chat: int, now: float) -> list[Message]:
        async with self.lock:
            rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
                AND sent_at>? AND deleted=0 AND own_alert=0 ORDER BY id""", (account, chat, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def active_batch(self, account: int, chat: int, now: float, after: int):
        async with self.lock:
            rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
                AND id>? AND sent_at>? AND deleted=0 AND own_alert=0 ORDER BY id LIMIT 100""",
                (account, chat, after, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def matching(self, account: int, ids: list[int], now: float) -> list[Message]:
        found = []
        for offset in range(0, len(ids), 100):
            batch = ids[offset:offset+100]
            if not batch:
                continue
            marks = ",".join("?" for _ in batch)
            async with self.lock:
                rows = await (await self.db.execute(f"""SELECT * FROM messages WHERE account=?
                    AND sent_at>? AND deleted=0 AND id IN ({marks})""", (account, now-TTL, *batch))).fetchall()
            found.extend(self.message(row) for row in rows)
        return found

    async def deleted(self, message: Message, now: float):
        await self.deleted_many([message], now)

    async def deleted_many(self, messages: list[Message], now: float):
        if not messages:
            return
        if len(messages) > 100:
            raise ValueError("Deletion transactions accept at most 100 messages")
        started = time.monotonic()
        keys = tuple(dict.fromkeys(self.key(message) for message in messages))
        marks = ",".join("(?,?,?)" for _ in keys)
        params = tuple(value for key in keys for value in key)
        async def operation():
            await self.db.execute(f"""UPDATE messages SET deleted=1
                WHERE (account,chat,id) IN ({marks}) AND sent_at>?""", (*params, now-TTL))
            queued = await self.db.execute(f"""INSERT OR IGNORE INTO deliveries(account,chat,id)
                SELECT account,chat,id FROM messages WHERE (account,chat,id) IN ({marks})
                AND deleted=1 AND sent_at>? AND kind='text' AND text IS NOT NULL AND own_alert=0""",
                (*params, now-TTL))
            return queued.rowcount
        with self.protect(keys):
            queued = await self._write(operation)
        log.info("Message deletion recorded batch=%s queued=%s elapsed_seconds=%.3f",
                 len(keys), queued, time.monotonic()-started)
        for account, chat, mid in keys:
            log.debug("Deletion batch member account=%s chat=%s message=%s", account, chat, mid)

    async def candidates(self, original: Message, now: float) -> list[Message]:
        async with self.lock:
            rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
                AND id<? AND sent_at>? AND own_alert=0 AND kind='text'
                AND text IS NOT NULL ORDER BY id DESC LIMIT 10""",
                (original.account, original.chat, original.id, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def due(self, account: int, now: float, groups=None, *, pin=False):
        params = [account, now, now-TTL, now, now]
        restriction = ""
        if groups is not None:
            if not groups:
                return None
            restriction = " AND d.chat IN (" + ",".join("?" for _ in groups) + ")"
            params.extend(groups)
        async with self.lock:
            row = await (await self.db.execute("""SELECT m.*,d.parts,d.next_part FROM deliveries d
                JOIN messages m USING(account,chat,id) WHERE d.account=? AND d.done=0
                AND d.next_at<=? AND m.sent_at>?
                AND COALESCE((SELECT until FROM cooldowns WHERE account=d.account),0)<=?
                AND COALESCE((SELECT until FROM chat_cooldowns
                    WHERE account=d.account AND chat=d.chat),0)<=?"""
                + restriction + " ORDER BY m.sent_at,m.id,m.chat LIMIT 1", params)).fetchone()
            if row is not None and pin:
                self.pin((row["account"], row["chat"], row["id"]))
            return row

    async def pending_count(self, account: int, now: float):
        async with self.lock:
            row = await (await self.db.execute("""SELECT count(*) FROM deliveries d
                JOIN messages m USING(account,chat,id)
                WHERE d.account=? AND d.done=0 AND m.sent_at>?""", (account, now-TTL))).fetchone()
        return row[0]

    async def reserve_send(self, message: Message, part: int, now: float) -> bool:
        """Persist pacing before a network attempt; a restart must not bypass it."""
        async def operation():
            eligible = await (await self.db.execute("""SELECT 1 FROM deliveries d
                JOIN messages m USING(account,chat,id)
                WHERE d.account=? AND d.chat=? AND d.id=? AND d.done=0 AND d.next_part=?
                AND d.next_at<=? AND m.sent_at>?
                AND COALESCE((SELECT until FROM cooldowns WHERE account=d.account),0)<=?
                AND COALESCE((SELECT until FROM chat_cooldowns
                    WHERE account=d.account AND chat=d.chat),0)<=?""",
                (*self.key(message), part, now, now-TTL, now, now))).fetchone()
            if eligible is None:
                return False
            await self.db.execute("""INSERT INTO cooldowns(account,until) VALUES(?,?)
                ON CONFLICT(account) DO UPDATE SET until=MAX(until,excluded.until)""",
                (message.account, now+1))
            await self.db.execute("""INSERT INTO chat_cooldowns(account,chat,until) VALUES(?,?,?)
                ON CONFLICT(account,chat) DO UPDATE SET until=MAX(until,excluded.until)""",
                (message.account, message.chat, now+3))
            return True
        reserved = await self._write(operation, self.key(message))
        if reserved:
            log.debug("Delivery pacing reserved account=%s chat=%s global_until=%s chat_until=%s",
                      message.account, message.chat, now+1, now+3)
        return reserved

    async def prepare(self, message: Message, parts: list[str]):
        payload = json.dumps(parts)
        async def operation():
            await self.db.execute("""UPDATE deliveries SET parts=? WHERE account=? AND chat=? AND id=?
                AND parts IS NULL""", (payload, *self.key(message)))
        await self._write(operation, self.key(message), reserve=len(payload.encode()) + 32 * 1024)

    async def pending_parts(self, account: int, chat: int, now: float) -> list[str]:
        async with self.lock:
            rows = await (await self.db.execute("""SELECT d.parts FROM deliveries d
                JOIN messages m USING(account,chat,id) WHERE d.account=? AND d.chat=?
                AND d.done=0 AND d.parts IS NOT NULL AND m.sent_at>?""",
                (account, chat, now-TTL))).fetchall()
        return [part for row in rows for part in json.loads(row["parts"])]

    async def postpone(self, message: Message, until: float, global_delay: bool = False):
        async def operation():
            await self.db.execute("UPDATE deliveries SET next_at=? WHERE account=? AND chat=? AND id=?",
                                  (until, *self.key(message)))
            if global_delay:
                await self.db.execute("""INSERT INTO cooldowns(account,until) VALUES(?,?)
                    ON CONFLICT(account) DO UPDATE SET until=MAX(until,excluded.until)""",
                    (message.account, until))
        await self._write(operation, self.key(message))

    async def delivered(self, message: Message, part: int, total: int, alert: Message, now: float):
        async def operation():
            await self.db.execute("""INSERT INTO messages
                (account,chat,id,author,name,username,sent_at,text,kind,own_alert)
                VALUES(:account,:chat,:id,:author,:name,:username,:sent_at,:text,:kind,1)
                ON CONFLICT(account,chat,id) DO UPDATE SET own_alert=1""", asdict(alert))
            await self.db.execute("""UPDATE deliveries SET next_part=?,done=?,next_at=?
                WHERE account=? AND chat=? AND id=?""",
                (part+1, int(part+1 == total), now, *self.key(message)))
        await self._write(operation, self.key(message))
