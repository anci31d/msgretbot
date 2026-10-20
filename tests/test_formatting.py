from html import unescape
import re
from msgretbot.formatting import render, units


def visible(text):
    return unescape(re.sub(r"<[^>]+>", "", text))


def test_html_and_no_username(message):
    parts = render(message(username=None, name='<Alice & "Bob">', text="<script>``` &"), [])
    assert '<a href="tg://user?id=20">&lt;Alice &amp; &quot;Bob&quot;&gt;</a>' in parts[0]
    assert "<pre>&lt;script&gt;``` &amp;</pre>" in parts[0]
    assert "Контекст отсутствует" in parts[0]


def test_long_unicode_preserved_within_telegram_limit(message):
    text = "😀<&\n```" * 1000
    context = [message(i, username=None, name="😀"*100, text="😀"*200) for i in range(3)]
    parts = render(message(text=text, username=None, name="😀"*100), context)
    assert all(units(visible(part)) <= 4096 for part in parts)
    recovered = "".join(unescape(re.search(r"<pre>(.*?)</pre>", part, re.S)[1]) for part in parts)
    assert recovered == text
    assert all(part.count("<pre>") == part.count("</pre>") == 1 for part in parts)


def test_missing_author(message):
    assert "Unknown user" in render(message(author=None, username=None, name=""), [])[0]
