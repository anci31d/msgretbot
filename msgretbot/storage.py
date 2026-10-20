"""SQLite persistence and transactional notification queue."""
import asyncio
from dataclasses import asdict
import json
import logging
from pathlib import Path
import aiosqlite
from .models import Message, TTL

log = logging.getLogger(__name__)

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
CREATE TABLE IF NOT EXISTS deliveries (
 account INTEGER NOT NULL, chat INTEGER NOT NULL, id INTEGER NOT NULL,
 parts TEXT, next_part INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
 done INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(account, chat, id),
 FOREIGN KEY(account, chat, id) REFERENCES messages(account, chat, id) ON DELETE CASCADE
);
PRAGMA user_version=1;
"""


class Store:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db
        self.lock = asyncio.Lock()

    @classmethod
    async def open(cls, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(path, isolation_level=None)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=5000")
            row = await (await db.execute("PRAGMA user_version")).fetchone()
            if row[0] > 1:
                raise ValueError("Database schema is newer than this application")
            await db.executescript(SCHEMA)
            log.info("Database opened schema=1 journal=WAL")
            return cls(db)
        except BaseException:
            await db.close()
            raise

    async def close(self):
        await self.db.close()

    @staticmethod
    def message(row) -> Message:
        return Message(**{key: row[key] for key in Message.__dataclass_fields__})

    async def save(self, message: Message, now: float):
        if message.sent_at <= now - TTL:
            log.debug("Expired message skipped chat=%s message=%s", message.chat, message.id)
            return
        values = asdict(message)
        async with self.lock:
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
            log.info("Message saved account=%s chat=%s message=%s kind=%s own_alert=%s revision=%s",
                     message.account, message.chat, message.id, message.kind,
                     bool(message.own_alert), message.revision_at)

    async def cleanup(self, now: float):
        async with self.lock:
            removed = await self.db.execute("DELETE FROM messages WHERE sent_at<=?", (now - TTL,))
            await self.db.execute("DELETE FROM cooldowns WHERE until<=?", (now,))
            if removed.rowcount:
                log.info("Expired messages purged count=%s; related deliveries removed", removed.rowcount)
            else:
                log.debug("Expiry cleanup completed count=0")

    async def active(self, account: int, chat: int, now: float) -> list[Message]:
        rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
            AND sent_at>? AND deleted=0 AND own_alert=0 ORDER BY id""", (account, chat, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def matching(self, account: int, ids: list[int], now: float) -> list[Message]:
        found = []
        for offset in range(0, len(ids), 100):
            batch = ids[offset:offset+100]
            if not batch:
                continue
            marks = ",".join("?" for _ in batch)
            rows = await (await self.db.execute(f"""SELECT * FROM messages WHERE account=?
                AND sent_at>? AND deleted=0 AND id IN ({marks})""", (account, now-TTL, *batch))).fetchall()
            found.extend(self.message(row) for row in rows)
        return found

    async def deleted(self, message: Message, now: float):
        async with self.lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                key = (message.account, message.chat, message.id)
                await self.db.execute("""UPDATE messages SET deleted=1
                    WHERE account=? AND chat=? AND id=? AND sent_at>?""", (*key, now-TTL))
                queued = await self.db.execute("""INSERT OR IGNORE INTO deliveries(account,chat,id)
                    SELECT account,chat,id FROM messages WHERE account=? AND chat=? AND id=?
                    AND deleted=1 AND sent_at>? AND kind='text' AND text IS NOT NULL AND own_alert=0""",
                    (*key, now-TTL))
                await self.db.commit()
                log.info("Message deletion recorded account=%s chat=%s message=%s queued=%s",
                         message.account, message.chat, message.id, bool(queued.rowcount))
            except BaseException:
                await self.db.rollback()
                raise

    async def candidates(self, original: Message, now: float) -> list[Message]:
        rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
            AND id<? AND sent_at>? AND deleted=0 AND own_alert=0 AND kind='text'
            AND text IS NOT NULL AND author IS NOT NULL ORDER BY id DESC LIMIT 10""",
            (original.account, original.chat, original.id, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def preceding(self, original: Message, now: float) -> list[Message]:
        rows = await (await self.db.execute("""SELECT * FROM messages WHERE account=? AND chat=?
            AND id<? AND sent_at>? AND deleted=0 AND own_alert=0 AND kind!='service'
            ORDER BY id DESC""", (original.account, original.chat, original.id, now-TTL))).fetchall()
        return [self.message(row) for row in rows]

    async def due(self, account: int, now: float, groups=None):
        params = [account, now, now-TTL, now]
        restriction = ""
        if groups is not None:
            if not groups:
                return None
            restriction = " AND d.chat IN (" + ",".join("?" for _ in groups) + ")"
            params.extend(groups)
        return await (await self.db.execute("""SELECT m.*,d.parts,d.next_part FROM deliveries d
            JOIN messages m USING(account,chat,id) WHERE d.account=? AND d.done=0
            AND d.next_at<=? AND m.sent_at>?
            AND COALESCE((SELECT until FROM cooldowns WHERE account=d.account),0)<=?"""
            + restriction + " ORDER BY m.sent_at,m.id LIMIT 1", params)).fetchone()

    async def prepare(self, message: Message, parts: list[str]):
        async with self.lock:
            await self.db.execute("""UPDATE deliveries SET parts=? WHERE account=? AND chat=? AND id=?
                AND parts IS NULL""", (json.dumps(parts), message.account, message.chat, message.id))

    async def pending_parts(self, account: int, chat: int, now: float) -> list[str]:
        rows = await (await self.db.execute("""SELECT d.parts FROM deliveries d
            JOIN messages m USING(account,chat,id) WHERE d.account=? AND d.chat=?
            AND d.done=0 AND d.parts IS NOT NULL AND m.sent_at>?""",
            (account, chat, now-TTL))).fetchall()
        return [part for row in rows for part in json.loads(row["parts"])]

    async def postpone(self, message: Message, until: float, global_delay: bool = False):
        async with self.lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.execute("UPDATE deliveries SET next_at=? WHERE account=? AND chat=? AND id=?",
                                      (until, message.account, message.chat, message.id))
                if global_delay:
                    await self.db.execute("""INSERT INTO cooldowns(account,until) VALUES(?,?)
                        ON CONFLICT(account) DO UPDATE SET until=MAX(until,excluded.until)""",
                        (message.account, until))
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise

    async def delivered(self, message: Message, part: int, total: int, alert: Message, now: float):
        async with self.lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.execute("""INSERT INTO messages
                    (account,chat,id,author,name,username,sent_at,text,kind,own_alert)
                    VALUES(:account,:chat,:id,:author,:name,:username,:sent_at,:text,:kind,1)
                    ON CONFLICT(account,chat,id) DO UPDATE SET own_alert=1""", asdict(alert))
                await self.db.execute("""UPDATE deliveries SET next_part=?,done=?,next_at=?
                    WHERE account=? AND chat=? AND id=?""",
                    (part+1, int(part+1 == total), now, message.account, message.chat, message.id))
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise
