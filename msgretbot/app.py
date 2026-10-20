"""Command-line application. No implicit interactive login during run."""
import argparse
import asyncio
import fcntl
import logging
import os
import time
from pathlib import Path
from aiogram import Bot
from telethon import TelegramClient, events
from .config import Config, LoggingConfig
from .logging_setup import configure_logging
from .service import Recovery
from .storage import Store
from .telegram import AiogramSender, TelegramReader, UserSender, normalize

log = logging.getLogger(__name__)


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
    queue = asyncio.Queue()
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
        service = Recovery(store, reader, sender, me.id, config.groups, sender_id)
        tasks.append(asyncio.create_task(service.cleaner()))

        async def on_message(event):
            if event.chat_id in service.groups:
                await queue.put(("message", normalize(event.message, me.id, event.chat_id)))
                log.debug("Message event queued chat=%s message=%s backlog=%s", event.chat_id,
                          event.message.id, queue.qsize())

        async def on_deleted(event):
            # Channel deletions cannot belong to a basic group.
            if event.chat_id is None or event.chat_id in service.groups:
                await queue.put(("deleted", (event.deleted_ids, event.chat_id)))

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

        # Register handlers before history reads; apply buffered live updates afterwards.
        await reader.bootstrap(service)
        consumer = asyncio.create_task(consume())
        tasks.append(consumer)
        await client.catch_up()
        joined = asyncio.create_task(queue.join())
        tasks.append(joined)
        done, _ = await asyncio.wait((joined, consumer), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        tasks.remove(joined)
        await service.reconcile()

        async def monitor():
            # Also covers reconnects too short to observe via connection-state polling.
            while True:
                await asyncio.sleep(60)
                if client.is_connected():
                    await service.reconcile()

        tasks.extend([asyncio.create_task(service.worker()),
                      asyncio.create_task(monitor()),
                      asyncio.create_task(client.run_until_disconnected())])
        log.info("Recovery service started mode=%s groups=%s", config.mode, len(config.groups))
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


def main():
    parser = argparse.ArgumentParser(description="Restore deleted text messages in basic Telegram groups")
    parser.add_argument("command", choices=("login", "run"))
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    args = parser.parse_args()
    configured = False
    try:
        config = Config.load(args.config)
        configure_logging(config.logging)
        configured = True
        asyncio.run(run(config, args.command))
    except KeyboardInterrupt:
        pass
    except Exception as error:
        if not configured:
            configure_logging(LoggingConfig())
        log.error("Startup or worker failed (%s); check configuration, connectivity and permissions",
                  type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
