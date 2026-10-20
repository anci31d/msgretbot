from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from telethon import types, errors
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from aiogram.methods import SendMessage
from msgretbot.models import Presence, RetryDelivery, PermanentDelivery
from msgretbot.telegram import TelegramReader, AiogramSender, UserSender, normalize


NOW = datetime.now(timezone.utc)


def group():
    return types.Chat(id=10, title="Test", photo=types.ChatPhotoEmpty(),
                      participants_count=3, date=NOW, version=1, left=False,
                      deactivated=False)


class Client:
    def __init__(self):
        self.call = AsyncMock(return_value=SimpleNamespace(chats=[group()]))
        self.get_messages = AsyncMock(return_value=[])

    async def __call__(self, request):
        return await self.call(request)


async def test_reader_presence_and_access_errors():
    client = Client()
    reader = TelegramReader(client, (-10,))
    reader.entities[-10] = group()
    reader.throttle = AsyncMock()
    client.get_messages.return_value = [SimpleNamespace(chat_id=-10), None]
    assert await reader.presence(-10, [1, 2]) == {1: Presence.PRESENT, 2: Presence.DELETED}
    client.call.side_effect = errors.ChatAdminRequiredError(None)
    assert await reader.presence(-10, [1, 2]) == {1: Presence.UNKNOWN, 2: Presence.UNKNOWN}
    client.get_messages.assert_awaited_once()


async def test_reader_leaving_group_and_wrong_peer_are_not_deletion():
    client = Client()
    reader = TelegramReader(client, (-10,))
    reader.entities[-10] = group()
    reader.throttle = AsyncMock()
    left = group()
    left.left = True
    client.call.return_value = SimpleNamespace(chats=[left])
    assert (await reader.presence(-10, [1]))[1] is Presence.UNKNOWN
    client.call.return_value = SimpleNamespace(chats=[group()])
    client.get_messages.return_value = [SimpleNamespace(chat_id=-20)]
    assert (await reader.presence(-10, [1]))[1] is Presence.UNKNOWN


async def test_reader_flood_wait_does_not_infer_deletion():
    client = Client()
    reader = TelegramReader(client, (-10,))
    reader.entities[-10] = group()
    reader.throttle = AsyncMock()
    client.call.side_effect = errors.FloodWaitError(None, capture=7)
    assert (await reader.presence(-10, [1]))[1] is Presence.UNKNOWN
    assert reader.next_request > 0


async def test_aiogram_sender_and_retry():
    bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=42)),
                          session=SimpleNamespace(close=AsyncMock()))
    sender = AiogramSender(bot)
    assert await sender.send(-10, "<pre>hello</pre>", "key") == 42
    assert bot.send_message.call_args.kwargs["parse_mode"] == "HTML"
    method = SendMessage(chat_id=-10, text="test")
    bot.send_message.side_effect = TelegramRetryAfter(method=method, message="wait", retry_after=9)
    with pytest.raises(RetryDelivery) as error:
        await sender.send(-10, "test", "key")
    assert error.value.delay == 9
    bot.send_message.side_effect = TelegramForbiddenError(method=method, message="forbidden")
    with pytest.raises(PermanentDelivery):
        await sender.send(-10, "test", "key")
    await sender.close()
    bot.session.close.assert_awaited_once()


async def test_user_sender_stable_random_id_and_html():
    client = Client()
    client.call.return_value = types.UpdateShortSentMessage(id=77, pts=1, pts_count=1, date=NOW)
    sender = UserSender(client, {-10: group()})
    for _ in range(2):
        assert await sender.send(-10, "<pre>&lt;hello&gt;</pre>", "stable-key") == 77
    first, second = [call.args[0] for call in client.call.call_args_list]
    assert first.random_id == second.random_id
    assert first.message == "<hello>"
    assert isinstance(first.entities[0], types.MessageEntityPre)
    client.call.side_effect = errors.FloodWaitError(None, capture=8)
    with pytest.raises(RetryDelivery) as error:
        await sender.send(-10, "test", "other-key")
    assert error.value.delay == 8


def raw(mid=1, **overrides):
    data = dict(sender=SimpleNamespace(first_name="A", last_name=None, username="a"),
                sender_id=20, id=mid, action=None, media=None, date=NOW, edit_date=None,
                message="text", photo=None, document=None)
    data.update(overrides)
    return SimpleNamespace(**data)


def test_media_normalization():
    assert normalize(raw(), 1, -10).text == "text"
    normalized = normalize(raw(media=object(), photo=object(), message="caption"), 1, -10)
    assert normalized.kind == "photo"
    assert normalized.text is None
    document = SimpleNamespace(attributes=[types.DocumentAttributeAudio(duration=1, voice=True)])
    assert normalize(raw(media=object(), document=document), 1, -10).kind == "voice"
    preview = types.MessageMediaWebPage(webpage=types.WebPageEmpty(id=1))
    assert normalize(raw(media=preview), 1, -10).kind == "text"


async def test_bootstrap_limit_and_ttl(setup):
    from datetime import timedelta
    from conftest import NOW as CLOCK
    from msgretbot.models import TTL
    class HistoryClient:
        def iter_messages(self, entity, limit):
            assert limit == 100
            async def iterate():
                yield raw(1, date=datetime.fromtimestamp(CLOCK-1, timezone.utc))
                yield raw(2, date=datetime.fromtimestamp(CLOCK-TTL, timezone.utc))
            return iterate()
    reader = TelegramReader(HistoryClient(), (-10,))
    reader.entities[-10] = group()
    reader.throttle = AsyncMock()
    await reader.bootstrap(setup.service)
    assert [m.id for m in await setup.store.active(1, -10, CLOCK)] == [1]


async def test_validate_rejects_non_basic_or_migrated_group():
    client = Client()
    client.get_entity = AsyncMock(return_value=SimpleNamespace(id=10))
    reader = TelegramReader(client, (-10,))
    reader.throttle = AsyncMock()
    with pytest.raises(ValueError):
        await reader.validate()
    migrated = group()
    migrated.migrated_to = types.InputChannel(channel_id=100, access_hash=1)
    client.get_entity.return_value = migrated
    with pytest.raises(ValueError):
        await reader.validate()
    client.get_entity.return_value = group()
    await reader.validate()
    assert -10 in reader.entities


async def test_incomplete_presence_response_is_unknown():
    client = Client()
    reader = TelegramReader(client, (-10,))
    reader.entities[-10] = group()
    reader.throttle = AsyncMock()
    client.get_messages.return_value = []
    assert (await reader.presence(-10, [1]))[1] is Presence.UNKNOWN


async def test_user_sender_prioritizes_random_id_over_unrelated_update():
    client = Client()
    async def respond(request):
        unrelated = types.Message(id=111, peer_id=types.PeerChat(chat_id=20),
                                  date=NOW, out=True, message="test")
        return types.Updates(updates=[types.UpdateNewMessage(message=unrelated, pts=1, pts_count=1),
                                     types.UpdateMessageID(id=222, random_id=request.random_id)],
                             users=[], chats=[], date=NOW, seq=1)
    client.call.side_effect = respond
    assert await UserSender(client, {-10: group()}).send(-10, "test", "key") == 222


async def test_user_sender_never_accepts_unrelated_outgoing_message():
    client = Client()
    unrelated = types.Message(id=111, peer_id=types.PeerChat(chat_id=20),
                              date=NOW, out=True, message="test")
    client.call.return_value = types.Updates(
        updates=[types.UpdateNewMessage(message=unrelated, pts=1, pts_count=1)],
        users=[], chats=[], date=NOW, seq=1)
    with pytest.raises(RetryDelivery):
        await UserSender(client, {-10: group()}).send(-10, "test", "key")


async def test_aiogram_sender_sets_reply_parameters():
    bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=42)))
    await AiogramSender(bot).send(-10, "alert", "key", reply_to=7)
    parameters = bot.send_message.call_args.kwargs["reply_parameters"]
    assert parameters.message_id == 7
    assert parameters.allow_sending_without_reply is True


async def test_aiogram_reply_deleted_between_check_and_send():
    from aiogram.exceptions import TelegramBadRequest
    method = SendMessage(chat_id=-10, text="alert")
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=[
        TelegramBadRequest(method=method, message="Bad Request: message to be replied not found"),
        SimpleNamespace(message_id=42)]))
    assert await AiogramSender(bot).send(-10, "alert", "key", reply_to=7) == 42
    assert bot.send_message.call_args_list[1].kwargs["reply_parameters"] is None


async def test_user_sender_reply_target_and_disappearing_reply():
    client = Client()
    client.call.side_effect = [errors.MessageIdInvalidError(None),
                              types.UpdateShortSentMessage(id=77, pts=1, pts_count=1, date=NOW)]
    assert await UserSender(client, {-10: group()}).send(-10, "alert", "key", reply_to=7) == 77
    first, second = [call.args[0] for call in client.call.call_args_list]
    assert first.reply_to.reply_to_msg_id == 7
    assert second.reply_to is None
    assert first.random_id == second.random_id


def supergroup(**changes):
    values = dict(id=100, title="Test", photo=types.ChatPhotoEmpty(), date=NOW,
                  megagroup=True, access_hash=1)
    values.update(changes)
    return types.Channel(**values)


@pytest.mark.parametrize("changes", [{}, {"forum": True}])
async def test_validate_supergroup(changes):
    client = Client()
    client.get_entity = AsyncMock(return_value=supergroup(**changes))
    reader = TelegramReader(client, (-1000000000100,))
    reader.throttle = AsyncMock()
    await reader.validate()
    assert reader.entities[-1000000000100].megagroup


@pytest.mark.parametrize("changes", [dict(megagroup=False, broadcast=True),
                                      dict(left=True), dict(gigagroup=True)])
async def test_validate_rejects_inaccessible_or_broadcast_channel(changes):
    client = Client()
    client.get_entity = AsyncMock(return_value=supergroup(**changes))
    reader = TelegramReader(client, (-1000000000100,))
    reader.throttle = AsyncMock()
    with pytest.raises(ValueError):
        await reader.validate()


async def test_supergroup_presence_and_access_loss():
    from telethon import functions
    chat = -1000000000100
    client = Client()
    client.call.return_value = SimpleNamespace(chats=[supergroup()])
    client.get_messages.return_value = [SimpleNamespace(chat_id=chat), None]
    reader = TelegramReader(client, (chat,))
    reader.entities[chat] = supergroup()
    reader.throttle = AsyncMock()
    assert await reader.presence(chat, [1, 2]) == {1: Presence.PRESENT, 2: Presence.DELETED}
    assert isinstance(client.call.call_args.args[0], functions.channels.GetFullChannelRequest)
    client.call.side_effect = errors.ChannelPrivateError(None)
    assert await reader.presence(chat, [1, 2]) == {1: Presence.UNKNOWN, 2: Presence.UNKNOWN}
    client.get_messages.assert_awaited_once()


async def test_user_sender_supergroup_update_fallback():
    chat = -1000000000100
    client = Client()
    message = types.Message(id=77, peer_id=types.PeerChannel(channel_id=100),
                            date=NOW, out=True, message="alert")
    client.call.return_value = types.Updates(
        updates=[types.UpdateNewChannelMessage(message=message, pts=1, pts_count=1)],
        users=[], chats=[], date=NOW, seq=1)
    assert await UserSender(client, {chat: supergroup()}).send(chat, "alert", "key") == 77


async def test_telethon_supergroup_delete_event_has_scoped_chat():
    from telethon import events
    event = events.MessageDeleted.build(types.UpdateDeleteChannelMessages(
        channel_id=100, messages=[7, 8], pts=1, pts_count=2))
    assert event.chat_id == -1000000000100
    assert event.deleted_ids == [7, 8]
    unscoped = events.MessageDeleted.build(types.UpdateDeleteMessages(
        messages=[7], pts=1, pts_count=1))
    assert unscoped.chat_id is None


async def test_supergroup_left_and_wrong_peer_remain_unknown():
    chat = -1000000000100
    client = Client()
    reader = TelegramReader(client, (chat,))
    reader.entities[chat] = supergroup()
    reader.throttle = AsyncMock()
    client.call.return_value = SimpleNamespace(chats=[supergroup(left=True)])
    assert await reader.presence(chat, [7]) == {7: Presence.UNKNOWN}
    client.get_messages.assert_not_awaited()
    client.call.return_value = SimpleNamespace(chats=[supergroup()])
    client.get_messages.return_value = [SimpleNamespace(chat_id=-1000000000200)]
    assert await reader.presence(chat, [7]) == {7: Presence.UNKNOWN}
