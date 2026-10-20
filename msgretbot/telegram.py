"""Telegram adapters: user-session observation and two publishing transports."""
import asyncio
import hashlib
import logging
import time
from aiogram import Bot
from aiogram.exceptions import (TelegramRetryAfter, TelegramNetworkError,
                                TelegramServerError, TelegramForbiddenError, TelegramBadRequest)
from telethon import errors, functions, types, utils
from telethon.extensions import html as telegram_html
from .models import Message, Presence, RetryDelivery, PermanentDelivery

log = logging.getLogger(__name__)


def normalize(raw, account: int, chat: int) -> Message:
    user = raw.sender
    name = " ".join(filter(None, (getattr(user, "first_name", None),
                                  getattr(user, "last_name", None)))) or "Unknown user"
    if raw.action:
        kind = "service"
    elif raw.media:
        kind = "media"
        if raw.photo:
            kind = "photo"
        elif raw.document:
            kind = "document"
            for attribute in raw.document.attributes:
                if isinstance(attribute, types.DocumentAttributeSticker):
                    kind = "sticker"
                    break
                if isinstance(attribute, types.DocumentAttributeAudio):
                    kind = "voice" if attribute.voice else "audio"
                elif isinstance(attribute, types.DocumentAttributeVideo):
                    kind = "video_note" if attribute.round_message else "video"
    else:
        kind = "text"
    # A web preview is still an ordinary text message.
    if isinstance(raw.media, types.MessageMediaWebPage):
        kind = "text"
    return Message(account, chat, raw.id, raw.sender_id, name, getattr(user, "username", None),
                   raw.date.timestamp(), raw.message if kind == "text" else None, kind,
                   revision_at=(raw.edit_date or raw.date).timestamp())


class TelegramReader:
    def __init__(self, client, groups):
        self.client = client
        self.groups = groups
        self.entities = {}
        self.next_request = 0.0
        self.rate_lock = asyncio.Lock()

    async def throttle(self):
        async with self.rate_lock:
            await asyncio.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + 0.5

    async def validate(self):
        for group in self.groups:
            await self.throttle()
            entity = await self.client.get_entity(group)
            is_basic = isinstance(entity, types.Chat)
            is_supergroup = isinstance(entity, types.Channel) and entity.megagroup
            if ((not is_basic and not is_supergroup) or getattr(entity, "deactivated", False)
                    or entity.left or getattr(entity, "migrated_to", None)
                    or getattr(entity, "gigagroup", False) or getattr(entity, "broadcast", False)):
                raise ValueError("Configured destination must be an accessible group or supergroup")
            self.entities[group] = entity
            log.info("Group validated chat=%s", group)

    async def bootstrap(self, service):
        for chat, entity in self.entities.items():
            log.info("History loading chat=%s limit=100", chat)
            fetched = 0
            # iter_messages limits the raw history, not the count after TTL/type filtering.
            await self.throttle()
            async for raw in self.client.iter_messages(entity, limit=100):
                await service.record(normalize(raw, service.account, chat))
                fetched += 1
            log.info("History loaded chat=%s fetched=%s", chat, fetched)

    async def presence(self, chat: int, ids: list[int]) -> dict[int, Presence]:
        states = {mid: Presence.UNKNOWN for mid in ids}
        if chat not in self.entities or not ids:
            return states
        try:
            await self.throttle()
            entity = self.entities[chat]
            if isinstance(entity, types.Channel) and entity.megagroup:
                result = await self.client(functions.channels.GetFullChannelRequest(entity))
            else:
                # Validate membership first: basic-group getMessages is account-wide.
                result = await self.client(functions.messages.GetFullChatRequest(entity.id))
            entity = next((c for c in result.chats if c.id == self.entities[chat].id), None)
            valid = isinstance(entity, types.Chat) or (isinstance(entity, types.Channel) and entity.megagroup)
            if (not valid or entity.left or getattr(entity, "deactivated", False)
                    or getattr(entity, "migrated_to", None)):
                return states
            await self.throttle()
            messages = await self.client.get_messages(self.entities[chat], ids=ids)
            if len(messages) != len(ids):
                return states
            for mid, message in zip(ids, messages):
                if message is None or isinstance(message, types.MessageEmpty):
                    states[mid] = Presence.DELETED
                elif getattr(message, "chat_id", None) == chat:
                    states[mid] = Presence.PRESENT
        except errors.FloodWaitError as error:
            log.warning("Presence rate limited chat=%s wait_seconds=%s", chat, error.seconds)
            self.next_request = max(self.next_request, time.monotonic() + error.seconds)
        except (errors.RPCError, OSError, asyncio.TimeoutError) as error:
            log.warning("Message presence unavailable chat=%s count=%s error_type=%s; deletion not inferred",
                        chat, len(ids), type(error).__name__)
        log.debug("Presence checked chat=%s requested=%s present=%s deleted=%s unknown=%s", chat, len(ids),
                  sum(s is Presence.PRESENT for s in states.values()),
                  sum(s is Presence.DELETED for s in states.values()),
                  sum(s is Presence.UNKNOWN for s in states.values()))
        return states


class AiogramSender:
    def __init__(self, bot: Bot):
        self.bot = bot

    async def send(self, chat: int, html: str, delivery_key: str) -> int:
        try:
            message = await self.bot.send_message(chat, html, parse_mode="HTML",
                                                 link_preview_options={"is_disabled": True})
            return message.message_id
        except TelegramRetryAfter as error:
            raise RetryDelivery(error.retry_after) from None
        except (TelegramNetworkError, TelegramServerError, OSError, asyncio.TimeoutError):
            raise RetryDelivery(5) from None
        except TelegramBadRequest as error:
            log.warning("Bot delivery rejected chat=%s error_type=%s", chat, type(error).__name__)
            raise PermanentDelivery from None
        except TelegramForbiddenError:
            log.warning("Bot delivery forbidden chat=%s", chat)
            raise PermanentDelivery from None

    async def close(self):
        await self.bot.session.close()


class UserSender:
    def __init__(self, client, entities):
        self.client, self.entities = client, entities

    async def send(self, chat: int, html: str, delivery_key: str) -> int:
        text, entities = telegram_html.parse(html)
        # Stable per delivery part, including after a process restart.
        random_id = int.from_bytes(hashlib.sha256(delivery_key.encode()).digest()[:8], "big", signed=True)
        try:
            response = await self.client(functions.messages.SendMessageRequest(
                peer=self.entities[chat], message=text, random_id=random_id, entities=entities,
                no_webpage=True))
            if isinstance(response, types.UpdateShortSentMessage):
                return response.id
            for update in getattr(response, "updates", []):
                if isinstance(update, types.UpdateMessageID) and update.random_id == random_id:
                    return update.id
            matching = [update.message.id for update in getattr(response, "updates", [])
                        if isinstance(update, (types.UpdateNewMessage, types.UpdateNewChannelMessage)) and update.message.out
                        and utils.get_peer_id(update.message.peer_id) == chat
                        and update.message.message == text]
            if len(matching) == 1:
                return matching[0]
            raise RetryDelivery(5)
        except errors.FloodWaitError as error:
            raise RetryDelivery(error.seconds) from None
        except (errors.MessageIdInvalidError, errors.MsgIdInvalidError):
            raise PermanentDelivery from None
        except (errors.ServerError, OSError, asyncio.TimeoutError):
            raise RetryDelivery(5) from None
        except errors.RPCError as error:
            log.warning("User delivery rejected chat=%s error_type=%s", chat, type(error).__name__)
            raise PermanentDelivery from None

    async def close(self):
        pass
