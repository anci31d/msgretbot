"""Command-line application. No implicit interactive login during run."""
import argparse
import asyncio
import fcntl
import logging
import os
import signal
import time
from pathlib import Path
from aiogram import Bot
from telethon import TelegramClient, events
from .config import Config, LoggingConfig, InvalidGroupId
from .logging_setup import configure_logging
from .service import Recovery
from .storage import Store
from .telegram import AiogramSender, TelegramReader, UserSender, normalize

log = logging.getLogger(__name__)


async def enqueue_event(queue, event):
    if queue.full():
        log.warning("Event queue full backlog=%s; applying backpressure", queue.qsize())
    await queue.put(event)
    log.debug("Event queued kind=%s backlog=%s", event[0], queue.qsize())


async def enqueue_deletions(queue, ids, chat):
    for offset in range(0, len(ids), 100):
        await enqueue_event(queue, ("deleted", (ids[offset:offset+100], chat)))


async def monitor_reconciliation(client, service, queue=None):
    """Also cover reconnects too short to observe through connection state."""
    while True:
        await asyncio.sleep(30)
        if client.is_connected():
            await service.reconcile()
        if queue is not None:
            log.info("Recovery load events=%s pending=%s", queue.qsize(),
                     await service.store.pending_count(service.account, service.clock()))


async def initialize_recovery(reader, service, client, queue, consume, tasks, mode):
    """Start ingestion and delivery before startup history checks can be rate limited."""
    consumer = asyncio.create_task(consume())
    tasks.extend((consumer, asyncio.create_task(service.worker())))
    log.info("Recovery service started mode=%s groups=%s", mode, len(service.groups))
    await reader.bootstrap(service)
    await client.catch_up()
    joined = asyncio.create_task(queue.join())
    tasks.append(joined)
    done, _ = await asyncio.wait((joined, consumer), return_when=asyncio.FIRST_COMPLETED)
    for task in done:
        task.result()
    tasks.remove(joined)
    await service.reconcile()


async def run(config: Config, command: str):
    log.info("Application starting command=%s mode=%s groups=%s", command, config.mode, len(config.groups))
    # Restrict session/database files created by SQLite and Telethon.
    os.umask(0o077)
    config.session.parent.mkdir(parents=True, exist_ok=True)
    instance_lock = Path(str(config.session) + ".lock").open("a")
    try:
        fcntl.flock(instance_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        instance_lock.close()
        raise ValueError("Another process is using this observer session") from None
    client = TelegramClient(str(config.session), config.api_id, config.api_hash,
                            flood_sleep_threshold=0, sequential_updates=True)
    store = sender = None
    queue = asyncio.Queue(maxsize=1000)
    tasks = []
    try:
        if command == "login":
            await client.start()
            me = await client.get_me()
            if me.bot:
                raise ValueError("Observer session must belong to a user")
            print("User session authorized. No group messages sent.")
            return
        await client.connect()
        log.info("Observer connected")
        if not await client.is_user_authorized():
            raise ValueError("Run the login command before starting the service")
        me = await client.get_me()
        if me.bot:
            raise ValueError("Observer session must belong to a user")
        log.info("Observer authorized account=%s", me.id)
        reader = TelegramReader(client, config.groups)
        await reader.validate()
        store = await Store.open(config.database)
        await store.cleanup(time.time())
        if config.mode == "hybrid":
            bot = Bot(config.bot_token)
            sender = AiogramSender(bot)
            publisher = await bot.get_me()
            sender_id = publisher.id
        else:
            sender, sender_id = UserSender(client, reader.entities), me.id
        service = Recovery(store, reader, sender, me.id, config.groups, sender_id,
                           alert_mode=config.alerts.mode)
        tasks.append(asyncio.create_task(service.cleaner()))

        async def on_message(event):
            if event.chat_id in service.groups:
                await enqueue_event(queue, ("message", normalize(event.message, me.id, event.chat_id)))
                log.debug("Message event queued chat=%s message=%s backlog=%s", event.chat_id,
                          event.message.id, queue.qsize())

        async def on_deleted(event):
            # For supergroups Telegram supplies the channel peer in chat_id.
            if event.chat_id is None or event.chat_id in service.groups:
                await enqueue_deletions(queue, event.deleted_ids, event.chat_id)

        client.add_event_handler(on_message, events.NewMessage())
        client.add_event_handler(on_message, events.MessageEdited())
        client.add_event_handler(on_deleted, events.MessageDeleted())

        async def consume():
            while True:
                kind, payload = await queue.get()
                try:
                    if kind == "message":
                        await service.record(payload)
                    else:
                        await service.deletion(*payload)
                finally:
                    queue.task_done()

        # Start the queue worker before any Telegram history or presence requests.
        await initialize_recovery(reader, service, client, queue, consume, tasks, config.mode)

        tasks.extend([asyncio.create_task(monitor_reconciliation(client, service, queue)),
                      asyncio.create_task(client.run_until_disconnected())])
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if sender:
            await sender.close()
        await client.disconnect()
        if store:
            await store.close()
        instance_lock.close()
        log.info("Application stopped")


async def serve(config: Config, command: str):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run(config, command)
    except asyncio.CancelledError:
        log.info("Termination requested; application resources closed")
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def main():
    parser = argparse.ArgumentParser(description="Restore deleted text messages in Telegram groups and supergroups")
    parser.add_argument("command", choices=("login", "run"))
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    args = parser.parse_args()
    configured = False
    try:
        config = Config.load(args.config)
        configure_logging(config.logging, secrets=(config.api_hash, config.bot_token))
        configured = True
        asyncio.run(serve(config, args.command))
    except KeyboardInterrupt:
        pass
    except Exception as error:
        if not configured:
            configure_logging(LoggingConfig())
        if isinstance(error, InvalidGroupId):
            log.error("Invalid configuration: %s", error)
        log.error("Startup or worker failed (%s); check configuration, connectivity and permissions",
                  type(error).__name__, exc_info=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
