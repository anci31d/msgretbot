"""Recovery orchestration, independent of Telegram transports."""
import asyncio
from dataclasses import replace
import json
import logging
import time
from .formatting import render, plain_text
from .models import Message, Presence, RetryDelivery, PermanentDelivery, TTL
from .storage import DatabaseCapacityError

log = logging.getLogger(__name__)


class Recovery:
    def __init__(self, store, reader, sender, account: int, groups: tuple[int, ...],
                 sender_id: int, clock=time.time, alert_mode: str = "loud"):
        self.store, self.reader, self.sender = store, reader, sender
        self.account, self.groups, self.sender_id = account, frozenset(groups), sender_id
        self.silent_alerts = alert_mode == "silent"
        self.clock = clock
        self.lock = asyncio.Lock()
        self.next_send_at = 0.0
        self.delivery_lock = asyncio.Lock()
        self.receipt = None

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

    async def mark_deleted(self, messages):
        for offset in range(0, len(messages), 100):
            batch = messages[offset:offset+100]
            while True:
                now = self.clock()
                batch = [message for message in batch if message.sent_at > now-TTL]
                if not batch:
                    break
                try:
                    async with self.lock:
                        await self.store.deleted_many(batch, now)
                    break
                except DatabaseCapacityError:
                    log.warning("Deletion batch capacity unavailable count=%s retry_seconds=60", len(batch))
                    await asyncio.sleep(60)
            await asyncio.sleep(0)

    async def deletion(self, ids: list[int], chat: int | None = None):
        log.debug("Deletion event received chat=%s count=%s", chat, len(ids))
        for offset in range(0, len(ids), 100):
            messages = await self.store.matching(self.account, ids[offset:offset+100], self.clock())
            eligible = [message for message in messages if message.chat in self.groups
                        and (chat == message.chat if chat is not None
                             else message.chat > -1000000000000)]
            await self.mark_deleted(eligible)
            await asyncio.sleep(0)

    async def reconcile(self):
        log.debug("Reconciliation started groups=%s", len(self.groups))
        for chat in self.groups:
            after = 0
            while True:
                batch = await self.store.active_batch(self.account, chat, self.clock(), after)
                if not batch:
                    break
                after = batch[-1].id
                states = await self.reader.presence(chat, [m.id for m in batch])
                await self.mark_deleted([message for message in batch
                                         if states.get(message.id) is Presence.DELETED])
                await asyncio.sleep(0)
        log.debug("Reconciliation completed")

    async def context(self, message: Message):
        messages = await self.store.candidates(message, self.clock())
        messages.reverse()
        log.debug("Context selected chat=%s message=%s quotes=%s",
                  message.chat, message.id, len(messages))
        return messages

    async def deliver_once(self) -> bool:
        # Only publishers serialize here; readers and event ingestion never wait for Telegram.
        async with self.delivery_lock:
            if self.clock() < self.next_send_at:
                return False
            if self.receipt is not None:
                message = self.receipt[0]
                self.store.pin(self.store.key(message))
                row = None
            else:
                row = await self.store.due(self.account, self.clock(), self.groups, pin=True)
                if row is None:
                    return False
                message = self.store.message(row)
            try:
                if row is None:
                    await self.finish_receipt()
                    return True
                return await self.deliver_part(message, row)
            except DatabaseCapacityError:
                self.next_send_at = max(self.next_send_at, self.clock() + 60)
                log.warning("Alert database capacity unavailable chat=%s original=%s retry_seconds=60",
                            message.chat, message.id)
                try:
                    await self.store.postpone(message, self.next_send_at, global_delay=True)
                except DatabaseCapacityError:
                    log.warning("Capacity cooldown could not be persisted; in-memory pause remains active")
                return True
            finally:
                self.store.unpin(self.store.key(message))

    async def deliver_part(self, message, row):
        if row["parts"] is None:
            context = await self.context(message)
            if message.sent_at <= self.clock()-TTL:
                return True
            parts = render(message, context, silent=self.silent_alerts)
            await self.store.prepare(message, parts)
        else:
            parts = json.loads(row["parts"])
        index = row["next_part"]
        # Recheck expiry and eligibility after potentially slow context requests.
        if not await self.store.reserve_send(message, index, self.clock()):
            return True
        key = f"{message.account}:{message.chat}:{message.id}:{index}"
        log.info("Alert sending chat=%s original=%s part=%s/%s",
                 message.chat, message.id, index+1, len(parts))
        try:
            sent_id = await self.sender.send(message.chat, parts[index], key)
        except RetryDelivery as error:
            log.warning("Alert deferred chat=%s original=%s retry_seconds=%s",
                        message.chat, message.id, error.delay)
            self.next_send_at = self.clock() + error.delay
            await self.store.postpone(message, self.next_send_at, global_delay=True)
            return True
        except PermanentDelivery:
            log.warning("Alert unavailable chat=%s original=%s retry_seconds=60", message.chat, message.id)
            await self.store.postpone(message, self.clock() + 60)
            return True
        alert = Message(self.account, message.chat, sent_id, self.sender_id,
                        "MsgRetBot", None, self.clock(), None, "text", True)
        # A known server receipt needs only a database retry, not another publication.
        self.receipt = message, index, len(parts), alert
        await self.finish_receipt()
        return True

    async def finish_receipt(self):
        message, index, total, alert = self.receipt
        await self.store.delivered(message, index, total, alert, self.clock())
        self.receipt = None
        log.info("Alert sent chat=%s original=%s sent=%s part=%s/%s",
                 message.chat, message.id, alert.id, index+1, total)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Delivery backlog account=%s pending=%s", self.account,
                      await self.store.pending_count(self.account, self.clock()))

    async def worker(self):
        while True:
            if not await self.deliver_once():
                await asyncio.sleep(1)

    async def cleaner(self):
        while True:
            # Expiry must not wait for a network request or FloodWait backoff.
            await self.store.cleanup(self.clock())
            await asyncio.sleep(30)
