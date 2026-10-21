from html import unescape
import re
import pytest
from telethon import types
from telethon.extensions import html as telegram_html
from msgretbot.formatting import render, units
from msgretbot.formatting import QuoteContext


def visible(text):
    return unescape(re.sub(r"<[^>]+>", "", text))


def test_html_and_no_username(message):
    parts = render(message(username=None, name='<Alice & "Bob">', text="<script>``` &"), [])
    assert '<a href="tg://user?id=20">&lt;Alice &amp; &quot;Bob&quot;&gt;</a>' in parts[0]
    assert "<pre>&lt;script&gt;``` &amp;</pre>" in parts[0]
    assert "Контекст отсутствует" in parts[0]


def test_long_unicode_preserved_within_telegram_limit(message):
    text = "😀<&\n```" * 1000
    context = [message(i, username=None, name="A"*100, text="😀"*200) for i in range(10)]
    parts = render(message(text=text, username=None, name="😀"*100), context)
    assert all(units(visible(part)) <= 4096 for part in parts)
    recovered = "".join(unescape(re.search(r"<pre>(.*?)</pre>", part, re.S)[1]) for part in parts)
    assert recovered == text
    assert all(part.count("<pre>") == part.count("</pre>") == 1 for part in parts)


def test_missing_author(message):
    assert "Unknown user" in render(message(author=None, username=None, name=""), [])[0]


@pytest.mark.parametrize("length", [99, 100, 101, 200])
def test_quote_truncation_includes_ellipsis_in_hundred_characters(message, length):
    html = QuoteContext().render([message(text="😀" * length)])
    quote = unescape(re.search(r"<code>(.*?)</code>", html)[1])
    assert len(quote) == min(length, 100)
    assert quote.endswith("…") == (length > 100)


def test_context_profiles_and_quoted_mentions_are_not_mentions(message):
    html = QuoteContext().render([message(username="alice", text="Ask @bob <&>")])
    assert '<a href="https://t.me/alice">alice</a>' in html
    assert '<code>Ask @bob &lt;&amp;&gt;</code>' in html
    _, entities = telegram_html.parse(html)
    assert any(isinstance(entity, types.MessageEntityTextUrl) and
               entity.url == "https://t.me/alice" for entity in entities)
    assert any(isinstance(entity, types.MessageEntityCode) for entity in entities)
    assert not any(isinstance(entity, (types.MessageEntityMention,
                                      types.MessageEntityMentionName,
                                      types.InputMessageEntityMentionName)) for entity in entities)


def test_silent_render_has_no_mentions_or_profile_links(message):
    original = message(username="alice", name="Alice", text="ping @original")
    context = [message(1, username="bob", name="Bob", text="Ask @alice")]
    parts = render(original, context, silent=True)
    assert "Сообщение alice удалено:" in parts[0]
    assert "1. bob:" in parts[0]
    assert "@" not in parts[0].split("<pre>", 1)[0]
    assert "<a " not in parts[0]
    assert "tg://user" not in parts[0]
    _, entities = telegram_html.parse(parts[0])
    assert not any(isinstance(entity, (types.MessageEntityMention,
                                      types.MessageEntityMentionName,
                                      types.InputMessageEntityMentionName,
                                      types.MessageEntityTextUrl)) for entity in entities)


@pytest.mark.parametrize("name", ["😀", "\u200b", "\u202eAlice", "𒀀", "A\u0301", "<script>", ""])
def test_context_unsupported_names_use_placeholder(message, name):
    html = QuoteContext().render([message(username=None, name=name)])
    assert "1. Участник:" in html
    assert "tg://user" not in html


def test_context_without_username_keeps_plain_readable_name(message):
    html = QuoteContext().render([message(username=None, name="Алиса Alice")])
    assert "1. Алиса Alice:" in html
    assert '<a ' not in html


def test_context_invalid_username_cannot_inject_profile_url(message):
    html = QuoteContext().render([message(username='alice\" onclick=\"x', name="😀")])
    assert "1. Участник:" in html
    assert 'onclick' not in html
