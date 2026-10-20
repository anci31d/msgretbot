"""Recovery orchestration, independent of Telegram transports."""
import asyncio
from dataclasses import replace
import json
import logging
import time
from .formatting import render, plain_text
from .models import Message, Presence, RetryDelivery, PermanentDelivery, TTL

log = logging.getLogger(__name__)


class Recovery:
    def __init__(self, store, reader, sender, account: int, groups: tuple[int, ...],
                 sender_id: int, clock=time.time):
        self.store, self.reader, self.sender = store, reader, sender
        self.account, self.groups, self.sender_id = account, frozenset(groups), sender_id
        self.clock = clock
        self.lock = asyncio.Lock()
        self.next_send_at = 0.0

    async def record(self, message: Message):
        if message.chat not in self.groups or message.account != self.account:
            return
        async with self.lock:
            # A dedicated publishing bot emits only this application's alerts.
            if self.sender_id != self.account and message.author == self.sender_id:
                message = replace(message, own_alert=True)
            elif message.author == self.account and message.text is not None:
                pending = await self.store.pending_parts(self.account, message.chat, self.clock())
                if any(plain_text(part) == message.text.strip() for part in pending):
                    message = replace(message, own_alert=True)
            await self.store.save(message, self.clock())

    async def deletion(self, ids: list[int], chat: int | None = None):
        async with self.lock:
            log.debug("Deletion event received chat=%s count=%s", chat, len(ids))
            for message in await self.store.matching(self.account, ids, self.clock()):
                if message.chat in self.groups and (chat is None or chat == message.chat):
                    # Unscoped deletions address only the account-wide basic-chat ID space.
                    # Channel message IDs are local to each supergroup.
                    if chat is None and message.chat <= -1000000000000:
                        continue
                    await self.store.deleted(message, self.clock())

    async def reconcile(self):
        async with self.lock:
            log.debug("Reconciliation started groups=%s", len(self.groups))
            for chat in self.groups:
                messages = await self.store.active(self.account, chat, self.clock())
                for offset in range(0, len(messages), 100):
                    batch = messages[offset:offset+100]
                    states = await self.reader.presence(chat, [m.id for m in batch])
                    for message in batch:
                        if states.get(message.id) is Presence.DELETED:
                            await self.store.deleted(message, self.clock())
            log.debug("Reconciliation completed")

    async def context(self, message: Message):
        candidates = await self.store.candidates(message, self.clock())
        states = await self.reader.presence(message.chat, [m.id for m in candidates]) if candidates else {}
        available, selected, authors = [], [], set()
        for candidate in candidates:
            state = states.get(candidate.id, Presence.UNKNOWN)
            if state is Presence.DELETED:
                await self.store.deleted(candidate, self.clock())
            elif state is Presence.PRESENT:
                available.append(candidate)
                if candidate.author not in authors and len(selected) < 3:
                    selected.append(candidate)
                    authors.add(candidate.author)
        for candidate in available:
            if len(selected) == 3:
                break
            if candidate not in selected:
                selected.append(candidate)
        selected.sort(key=lambda item: item.id, reverse=True)
        log.debug("Context selected chat=%s message=%s window=%s quotes=%s authors=%s",
                  message.chat, message.id, len(candidates), len(selected),
                  len({item.author for item in selected}))
        return selected

    async def reply_target(self, message: Message) -> int | None:
        candidates = await self.store.preceding(message, self.clock())
        for offset in range(0, len(candidates), 100):
            batch = candidates[offset:offset+100]
            states = await self.reader.presence(message.chat, [m.id for m in batch])
            for candidate in batch:
                state = states.get(candidate.id, Presence.UNKNOWN)
                if state is Presence.PRESENT:
                    return candidate.id
                if state is Presence.DELETED:
                    await self.store.deleted(candidate, self.clock())
        return None

    async def deliver_once(self) -> bool:
        async with self.lock:
            if self.clock() < self.next_send_at:
                return False
            row = await self.store.due(self.account, self.clock(), self.groups)
            if row is None:
                return False
            message = self.store.message(row)
            if row["parts"] is None:
                parts = render(message, await self.context(message))
                await self.store.prepare(message, parts)
            else:
                parts = json.loads(row["parts"])
            index = row["next_part"]
            reply_to = await self.reply_target(message)
            # TTL must still hold after potentially slow context requests.
            if message.sent_at <= self.clock() - TTL:
                await self.store.cleanup(self.clock())
                return True
            key = f"{message.account}:{message.chat}:{message.id}:{index}"
            log.info("Alert sending chat=%s original=%s part=%s/%s reply_to=%s",
                     message.chat, message.id, index+1, len(parts), reply_to)
            try:
                sent_id = await self.sender.send(message.chat, parts[index], key, reply_to=reply_to)
            except RetryDelivery as error:
                log.warning("Alert deferred chat=%s original=%s retry_seconds=%s",
                            message.chat, message.id, error.delay)
                self.next_send_at = self.clock() + error.delay
                await self.store.postpone(message, self.clock() + error.delay, global_delay=True)
                return True
            except PermanentDelivery:
                log.warning("Alert unavailable chat=%s original=%s retry_seconds=60", message.chat, message.id)
                await self.store.postpone(message, self.clock() + 60)
                return True
            alert = Message(self.account, message.chat, sent_id, self.sender_id,
                            "MsgRetBot", None, self.clock(), None, "text", True)
            await self.store.delivered(message, index, len(parts), alert, self.clock())
            log.info("Alert sent chat=%s original=%s sent=%s part=%s/%s",
                     message.chat, message.id, sent_id, index+1, len(parts))
            return True

    async def worker(self):
        while True:
            if not await self.deliver_once():
                await asyncio.sleep(1)

    async def cleaner(self):
        while True:
            # Expiry must not wait for a network request or FloodWait backoff.
            await self.store.cleanup(self.clock())
            await asyncio.sleep(60)
