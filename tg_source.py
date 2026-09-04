"""
Вакансии из телеграм-каналов — через публичную веб-версию канала.

Читаем https://t.me/s/<канал> — ту же страницу, что открывается в браузере
у любого прохожего. Ни аккаунта, ни api_id, ни файла сессии не нужно.

Почему не Telethon: он читает каналы от имени обычного аккаунта, а значит
на сервер пришлось бы положить файл сессии — это полный доступ к аккаунту
(читать переписки, писать от вашего имени). Ради публичных каналов такой
размен не нужен.

Ограничение подхода: так видны только публичные каналы с включённым
предпросмотром. Закрытые и те, где предпросмотр выключен, не читаются —
для них аккаунт всё же обязателен.

Каналы задаются переменной окружения TG_CHANNELS через запятую:
    TG_CHANNELS=@ux_jobs,@researcher_jobs

Формат возвращаемых словарей — общий для всех источников (см. search.py).
"""

import asyncio
import html as html_mod
import logging
import os
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Одна страница предпросмотра отдаёт последние ~20 постов.
_BLOCK = re.compile(r'data-post="([^"]+)"(.*?)(?=data-post="|\Z)', re.S)
_TEXT = re.compile(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.S)
_DATE = re.compile(r'<time datetime="([^"]+)"')

_HASHTAG = re.compile(r"#\S+")
_LEADING_JUNK = re.compile(r"^[\W_]+", re.U)
_SALARY_LABELLED = re.compile(
    r"(?:з/?п|зарплата|salary|доход|вилка|оклад)\s*[:\-—]?\s*([^\n]{3,60})", re.I
)
_MONEY = re.compile(
    r"(?:от\s*)?\d[\d\s]{2,}(?:\s*(?:-|–|—|до)\s*\d[\d\s]*)?\s*"
    r"(?:руб|₽|rub|\$|usd|€|eur|тыс|k\b)",
    re.I,
)
_RANGE = re.compile(r"(\d[\d\s]*)\s*(?:-|–|—|до)\s*(\d[\d\s]*)")
_REMOTE = re.compile(r"удал[её]нн|удал[её]нк|remote|можно из дома", re.I)
_HYBRID = re.compile(r"гибрид|hybrid|частично удал", re.I)
_OFFICE = re.compile(r"\bофис|on-?site|в офисе", re.I)


def channels():
    return [c.strip().lstrip("@") for c in os.getenv("TG_CHANNELS", "").split(",")
            if c.strip()]


def configured():
    """Источник работает, как только заданы каналы — ключи не нужны."""
    return bool(channels())


def _work_format(text):
    formats = []
    if _HYBRID.search(text):
        formats.append("hybrid")
    elif _REMOTE.search(text):
        formats.append("remote")
    if _OFFICE.search(text) and "hybrid" not in formats:
        formats.append("office")
    return ",".join(formats)


def _title(text):
    """Название вакансии — первая содержательная строка поста."""
    for line in text.splitlines():
        line = _LEADING_JUNK.sub("", _HASHTAG.sub("", line)).strip()
        if len(line) >= 4:
            return line[:120]
    return ""


def _int(text):
    digits = re.sub(r"\D", "", text or "")
    if not digits:
        return None
    value = int(digits)
    # «250к» и «250 тыс» в постах означают тысячи.
    return value * 1000 if value < 1000 else value


def _salary(text):
    """Зарплата из поста. Возвращает (from, to) — как у остальных источников."""
    m = _SALARY_LABELLED.search(text)
    chunk = m.group(1) if m else None
    if not chunk:
        m = _MONEY.search(text)
        chunk = m.group(0) if m else None
    if not chunk:
        return None, None

    rng = _RANGE.search(chunk)
    if rng:
        return _int(rng.group(1)), _int(rng.group(2))
    single = re.search(r"\d[\d\s]*", chunk)
    if not single:
        return None, None
    value = _int(single.group(0))
    if value and re.search(r"\bот\b", chunk, re.I):
        return value, None
    if value and re.search(r"\bдо\b", chunk, re.I):
        return None, value
    return value, value


def _post_text(chunk):
    """Текст поста: собираем все блоки, <br> превращаем в переводы строк."""
    parts = _TEXT.findall(chunk)
    if not parts:
        return ""
    raw = "\n".join(parts)
    raw = re.sub(r"<br\s*/?>", "\n", raw)
    raw = re.sub(r"</?(p|div)[^>]*>", "\n", raw)
    text = html_mod.unescape(re.sub(r"<[^>]+>", "", raw))
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def parse_channel_page(html, channel, words):
    """Разобрать страницу предпросмотра канала в список вакансий."""
    out = []
    for post_id, chunk in _BLOCK.findall(html):
        text = _post_text(chunk)
        if not text:
            continue
        if words and not any(w in text.lower() for w in words):
            continue

        title = _title(text)
        if not title:
            continue

        published = None
        date_m = _DATE.search(chunk)
        if date_m:
            try:
                published = datetime.fromisoformat(date_m.group(1)).astimezone(
                    MSK).isoformat(timespec="seconds")
            except ValueError:
                published = None

        salary_from, salary_to = _salary(text)
        out.append({
            "uid": "tg:" + post_id,
            "source": "tg",
            "ext_id": post_id,
            "title": title,
            "company": "",
            "area": "",
            "url": "https://t.me/" + post_id,
            "published_at": published,
            "salary_from": salary_from,
            "salary_to": salary_to,
            "currency": "RUR",
            "work_format": _work_format(text),
            "experience": "",
        })
    return out


async def fetch_telegram(client, queries):
    """Собрать вакансии из публичных каналов, указанных в TG_CHANNELS."""
    chans = channels()
    if not chans:
        return []

    words = [w.lower() for q in queries for w in re.split(r"\s+", q) if len(w) > 2]
    out = []
    for channel in chans:
        try:
            resp = await client.get(f"https://t.me/s/{channel}",
                                    headers={"User-Agent": UA})
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("телеграм: канал @%s не прочитан: %s", channel, exc)
            continue

        found = parse_channel_page(resp.text, channel, words)
        if not found and "tgme_widget_message" not in resp.text:
            logger.warning(
                "телеграм: у @%s нет публичного предпросмотра — "
                "закрытые каналы так не читаются", channel
            )
        out += found
    return out
