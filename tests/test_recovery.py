from dataclasses import replace
import json
import pytest
from msgretbot.models import Presence, RetryDelivery, PermanentDelivery, TTL
from msgretbot.service import Recovery
from msgretbot.storage import Store
from conftest import NOW


async def test_filters_and_last_revision(setup, message):
    for change in ({"chat": -30}, {"account": 2}, {"sent_at": NOW-TTL}):
        await setup.service.record(message(**change))
    assert await setup.store.active(1, -10, NOW) == []
    await setup.service.record(message(revision_at=100))
    await setup.service.record(message(text="edited", revision_at=200))
    await setup.service.record(message(text="stale history", revision_at=100))
    saved = (await setup.store.active(1, -10, NOW))[0]
    assert saved.text == "edited"
    assert saved.sent_at == NOW-100


async def test_batch_delete_without_chat_and_dedup(setup, message):
    await setup.service.record(message())
    await setup.service.record(message(11, chat=-20))
    await setup.service.deletion([10, 11, 999])
    await setup.service.deletion([10, 11])
    assert await setup.service.deliver_once()
    assert await setup.service.deliver_once()
    assert not await setup.service.deliver_once()
    assert {call[0] for call in setup.sender.calls} == {-10, -20}
    assert "Сообщение @alice удалено:" in setup.sender.calls[0][1]


async def test_chat_scoped_deletion(setup, message):
    await setup.service.record(message())
    await setup.service.deletion([10], -20)
    assert not await setup.service.deliver_once()


async def test_context_different_authors_and_presence(setup, message):
    for mid, author in [(1, 30), (2, 31), (3, 20), (4, 20), (5, 32), (6, 33)]:
        await setup.service.record(message(mid, author=author, text=f"text-{mid}"))
    setup.reader.states.update({5: Presence.DELETED, 6: Presence.UNKNOWN})
    context = await setup.service.context(message())
    assert [m.id for m in context] == [4, 2, 1]
    assert len({m.author for m in context}) == 3
    assert await setup.store.due(1, NOW) is not None  # Deleted context is also recovered.


async def test_unknown_not_deleted_on_reconciliation(setup, message):
    for mid in (10, 11, 12):
        await setup.service.record(message(mid))
    setup.reader.states.update({10: Presence.DELETED, 11: Presence.UNKNOWN})
    await setup.service.reconcile()
    assert [m.id for m in await setup.store.active(1, -10, NOW)] == [11, 12]
    assert (await setup.store.due(1, NOW))["id"] == 10


async def test_media_and_bot_alert_not_recovered(setup, message):
    await setup.service.record(message(kind="voice", text=None))
    await setup.service.record(message(11, author=99))
    await setup.service.deletion([10, 11])
    assert not await setup.service.deliver_once()
    assert await setup.service.context(message(12)) == []


async def test_userbot_alert_late_event_does_not_create_loop(setup, message):
    setup.service.sender_id = 1
    await setup.service.record(message())
    await setup.service.deletion([10])
    await setup.service.deliver_once()
    await setup.service.record(message(5001, author=1, text="delivered alert"))
    await setup.service.deletion([5001])
    assert not await setup.service.deliver_once()


async def test_expiry_removes_pending_payload_and_ttl_is_not_extended(setup, message):
    await setup.service.record(message(sent_at=NOW-TTL+1))
    await setup.service.deletion([10])
    await setup.store.prepare(message(), ["sensitive payload"])
    setup.clock[0] += 1
    assert await setup.store.due(1, setup.clock[0]) is None
    await setup.store.cleanup(setup.clock[0])
    for table in ("messages", "deliveries"):
        assert (await (await setup.store.db.execute(f"SELECT count(*) FROM {table}")).fetchone())[0] == 0


@pytest.mark.parametrize("error,delay", [(RetryDelivery(7), 7), (PermanentDelivery(), 60)])
async def test_retry_persists_and_waits(setup, message, error, delay):
    await setup.service.record(message())
    await setup.service.deletion([10])
    setup.sender.error = error
    assert await setup.service.deliver_once()
    assert not await setup.service.deliver_once()
    setup.clock[0] += delay
    setup.sender.error = None
    assert await setup.service.deliver_once()
    assert len(setup.sender.calls) == 1


async def test_restart_resumes_next_part(setup, message):
    await setup.service.record(message(text="x" * 6000))
    await setup.service.deletion([10])
    await setup.service.deliver_once()
    other = await Store.open(setup.path)
    try:
        resumed = Recovery(other, setup.reader, setup.sender, 1, (-10,), 99, clock=lambda: NOW)
        while await resumed.deliver_once():
            pass
    finally:
        await other.close()
    calls = setup.sender.calls
    assert len(calls) == 3
    assert len({call[2] for call in calls}) == 3
    assert calls[1][2].endswith(":1")


async def test_short_context_and_no_context(setup, message):
    await setup.service.record(message(1, author=30))
    context = await setup.service.context(message())
    assert len(context) == 1
    assert await setup.service.context(message(0)) == []


async def test_account_isolation(setup, message):
    await setup.store.save(message(account=2), NOW)
    await setup.service.deletion([10])
    assert await setup.store.due(1, NOW) is None
    assert len(await setup.store.active(2, -10, NOW)) == 1


async def test_sqlite_migration_version(setup):
    row = await (await setup.store.db.execute("PRAGMA user_version")).fetchone()
    assert row[0] == 1
    row = await (await setup.store.db.execute("PRAGMA journal_mode")).fetchone()
    assert row[0] == "wal"


async def test_userbot_lost_ack_alert_recognized_from_durable_payload(setup, message):
    from msgretbot.formatting import render, plain_text
    setup.service.sender_id = 1
    original = message()
    await setup.service.record(original)
    await setup.service.deletion([10])
    parts = render(original, [])
    await setup.store.prepare(original, parts)
    # The server delivered it, but the sending request timed out before an ID was saved.
    await setup.service.record(message(100, author=1, text=plain_text(parts[0])))
    assert all(m.id != 100 for m in await setup.store.active(1, -10, NOW))
    await setup.service.deletion([100])
    rows = await (await setup.store.db.execute("SELECT id FROM deliveries ORDER BY id")).fetchall()
    assert [row[0] for row in rows] == [10]


async def test_global_retry_cooldown(setup, message):
    for mid in (10, 11):
        await setup.service.record(message(mid))
    await setup.service.deletion([10, 11])
    setup.sender.error = RetryDelivery(20)
    await setup.service.deliver_once()
    setup.sender.error = None
    assert not await setup.service.deliver_once()
    assert setup.sender.calls == []
    setup.clock[0] += 20
    assert await setup.service.deliver_once()


async def test_restart_preserves_global_cooldown(setup, message):
    for mid in (10, 11):
        await setup.service.record(message(mid))
    await setup.service.deletion([10, 11])
    setup.sender.error = RetryDelivery(120)
    await setup.service.deliver_once()
    setup.sender.error = None
    restarted = Recovery(setup.store, setup.reader, setup.sender, 1, (-10,), 99,
                         clock=lambda: NOW+1)
    assert not await restarted.deliver_once()
    assert setup.sender.calls == []


async def test_pending_alert_never_sent_to_removed_group(setup, message):
    await setup.service.record(message())
    await setup.service.deletion([10])
    restarted = Recovery(setup.store, setup.reader, setup.sender, 1, (-20,), 99, clock=lambda: NOW)
    assert not await restarted.deliver_once()
    assert setup.sender.calls == []


async def test_context_three_messages_from_one_author(setup, message):
    for mid in range(1, 5):
        await setup.service.record(message(mid))
    context = await setup.service.context(message())
    assert [item.id for item in context] == [4, 3, 2]


async def test_context_two_authors_fills_nearest_extra_message(setup, message):
    for mid, author in [(1, 30), (2, 20), (3, 20), (4, 20)]:
        await setup.service.record(message(mid, author=author))
    assert [item.id for item in await setup.service.context(message())] == [4, 3, 1]


async def test_context_does_not_search_beyond_ten_texts(setup, message):
    for mid in range(1, 13):
        await setup.service.record(message(mid, author=30 if mid == 1 else 20))
    result = await setup.service.context(message(13))
    assert [item.id for item in result] == [12, 11, 10]
    assert len(setup.reader.requests[-1][1]) == 10
    assert 1 not in setup.reader.requests[-1][1]


async def test_media_does_not_consume_text_context_window(setup, message):
    await setup.service.record(message(1, author=30))
    for mid in range(2, 20):
        await setup.service.record(message(mid, kind="photo", text=None))
    await setup.service.record(message(20, author=20))
    assert [item.id for item in await setup.service.context(message(21))] == [20, 1]


async def test_reply_is_nearest_previous_message_including_media(setup, message):
    await setup.service.record(message(8))
    await setup.service.record(message(9, kind="photo", text=None))
    await setup.service.record(message())
    await setup.service.deletion([10])
    await setup.service.deliver_once()
    assert setup.sender.calls[0][3] == 9


async def test_reply_skips_deleted_unknown_and_own_alerts(setup, message):
    for mid in range(1, 5):
        await setup.service.record(message(mid))
    await setup.service.record(message(5, author=99))
    setup.reader.states.update({4: Presence.UNKNOWN, 3: Presence.DELETED})
    assert await setup.service.reply_target(message()) == 2


async def test_reply_absent_when_no_confirmed_predecessor(setup, message):
    await setup.service.record(message())
    await setup.service.deletion([10])
    await setup.service.deliver_once()
    assert setup.sender.calls[0][3] is None


async def test_operational_logs_exclude_message_contents(setup, message, caplog):
    caplog.set_level("DEBUG", logger="msgretbot")
    await setup.service.record(message(text="DO-NOT-LOG-TEXT", name="DO-NOT-LOG-NAME"))
    await setup.service.deletion([10])
    await setup.service.deliver_once()
    setup.clock[0] += TTL
    await setup.store.cleanup(setup.clock[0])
    assert "Message saved" in caplog.text
    assert "Message deletion recorded" in caplog.text
    assert "Alert sent" in caplog.text
    assert "Expired messages purged" in caplog.text
    assert "DO-NOT-LOG" not in caplog.text
