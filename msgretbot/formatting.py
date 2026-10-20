"""Safe Telegram HTML, sized by UTF-16 code units."""

from html import escape
from html.parser import HTMLParser
from .models import Message


def plain_text(html: str) -> str:
    """Extract the visible payload to recognize outgoing alerts after a lost ACK."""

    class Text(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts = []

        def handle_data(self, data):
            self.parts.append(data)

    parser = Text()
    parser.feed(html)
    return "".join(parser.parts).strip()


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


def author(message: Message) -> str:
    if message.username:
        return "@" + escape(message.username)
    name = escape((message.name or "Unknown user")[:100])
    if message.author is None:
        return name
    return f'<a href="tg://user?id={message.author}">{name}</a>'


class QuoteContext:
    """Render portable quotes; supergroup links can be added by a future formatter."""

    def render(self, messages: list[Message]) -> str:
        if not messages:
            return "Контекст отсутствует"
        lines = ["Контекст:"]
        for index, message in enumerate(messages, 1):
            text = message.text or ""
            quote = text[:160] + ("…" if len(text) > 160 else "")
            lines.append(f"{index}. {author(message)}: {escape(quote)}")
        return "\n".join(lines)


def render(message: Message, context: list[Message], formatter=None) -> list[str]:
    formatter = formatter or QuoteContext()
    header = f"!!! 1984 АЛЕРТ !!!\nСообщение {author(message)} удалено:\n"
    # Reserve enough space even for names and quotes made entirely of astral emoji.
    texts = chunks(message.text or "", 2100)
    parts = [header + "<pre>" + escape(text) + "</pre>" for text in texts]
    parts[-1] += "\n" + formatter.render(context)
    return parts
