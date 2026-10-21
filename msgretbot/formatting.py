"""Safe Telegram HTML, sized by UTF-16 code units."""

from html import escape
from html.parser import HTMLParser
import re
from .models import Message


def plain_text(html: str, *, strip: bool = True) -> str:
    """Extract the visible payload to recognize outgoing alerts after a lost ACK."""

    class Text(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts = []

        def handle_data(self, data):
            self.parts.append(data)

    parser = Text()
    parser.feed(html)
    text = "".join(parser.parts)
    return text.strip() if strip else text


def units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def chunks(text: str, limit: int) -> list[str]:
    result, buffer, size = [], [], 0
    for char in text:
        width = units(char)
        if size + width > limit:
            result.append("".join(buffer))
            buffer, size = [], 0
        buffer.append(char)
        size += width
    if buffer or not result:
        result.append("".join(buffer))
    return result


def author(message: Message, *, silent: bool = False) -> str:
    if silent:
        if message.username and re.fullmatch(r"[A-Za-z0-9_]{1,32}", message.username):
            return escape(message.username)
        return escape((message.name or "Unknown user")[:100])
    if message.username:
        return "@" + escape(message.username)
    name = escape((message.name or "Unknown user")[:100])
    if message.author is None:
        return name
    return f'<a href="tg://user?id={message.author}">{name}</a>'


def context_author(message: Message, *, silent: bool = False) -> str:
    """Use ordinary profile URLs instead of notification-producing mention entities."""
    if message.username and re.fullmatch(r"[A-Za-z0-9_]{1,32}", message.username):
        if silent:
            return escape(message.username)
        return f'<a href="https://t.me/{message.username}">{message.username}</a>'
    # Without a public username Telegram only offers ID mentions, which can notify.
    name = (message.name or "Участник")[:100]
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁё0-9 _.'-]+", name):
        name = "Участник"
    return escape(name)


class QuoteContext:
    """Render portable quotes; supergroup links can be added by a future formatter."""

    def __init__(self, *, silent: bool = False):
        self.silent = silent

    def render(self, messages: list[Message]) -> str:
        if not messages:
            return "Контекст отсутствует"
        lines = ["Контекст:"]
        for index, message in enumerate(messages, 1):
            text = message.text or ""
            quote = text[:99] + "…" if len(text) > 100 else text
            lines.append(f"{index}. {context_author(message, silent=self.silent)}: "
                         f"<code>{escape(quote)}</code>")
        return "\n".join(lines)


def render(message: Message, context: list[Message], formatter=None, *, silent: bool = False) -> list[str]:
    formatter = formatter or QuoteContext(silent=silent)
    header = f"!!! 1984 АЛЕРТ !!!\nСообщение {author(message, silent=silent)} удалено:\n"
    context_html = "\n" + formatter.render(context)
    # Ten quotes can consume most of Telegram's UTF-16 budget.
    limit = min(2100, 4096 - units(plain_text(header + context_html, strip=False)))
    if limit < 2:
        raise ValueError("Alert header and context exceed Telegram's message limit")
    texts = chunks(message.text or "", limit)
    parts = [header + "<pre>" + escape(text) + "</pre>" for text in texts]
    parts[-1] += context_html
    return parts
