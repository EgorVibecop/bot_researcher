"""
Вакансии из телеграм-каналов.

Ограничение платформы: бот не может читать канал, в котором он не
администратор. Поэтому каналы читаются от имени обычного аккаунта через
Telethon — для этого нужны api_id и api_hash с https://my.telegram.org
и однократный вход по телефону (создаётся файл сессии).

Переменные окружения:
    TG_API_ID, TG_API_HASH — с my.telegram.org
    TG_CHANNELS            — каналы через запятую: @hr_jobs,@ux_vacancies
    TG_SESSION             — имя файла сессии (по умолчанию tg_reader)

Если переменные не заданы, источник просто выключен и остальные работают
как обычно.

Формат возвращаемых словарей тот же, что у остальных источников
(см. шапку search.py), чтобы склейка дублей и оформление карточки
работали одинаково для всех.
"""

import asyncio
import logging
import os
import re
from datetime import timezone

logger = logging.getLogger(__name__)

# Сколько последних сообщений просматриваем в каждом канале.
SCAN_LIMIT = 200

_HASHTAG = re.compile(r"#\S+")
_LEADING_JUNK = re.compile(r"^[\W_]+", re.U)
_VACANCY_HINT = re.compile(
    r"вакансия|ищем|требуется|в поиске|hiring|we are looking|открыта позиция", re.I
)
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


def configured():
    """Настроен ли источник."""
    if not (os.getenv("TG_API_ID") and os.getenv("TG_API_HASH") and channels()):
        return False
    try:
        import telethon  # noqa: F401
    except ImportError:
        logger.warning("TG_API_ID задан, но telethon не установлен")
        return False
    return True


def channels():
    return [c.strip() for c in os.getenv("TG_CHANNELS", "").split(",") if c.strip()]


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


def _salary(text):
    """Вытащить зарплату. Возвращает (from, to) — как у остальных источников."""
    chunk = None
    m = _SALARY_LABELLED.search(text)
    if m:
        chunk = m.group(1)
    else:
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


def _int(text):
    digits = re.sub(r"\D", "", text or "")
    if not digits:
        return None
    value = int(digits)
    # «250к» и «250 тыс» в постах означают тысячи.
    return value * 1000 if value < 1000 else value


async def fetch_telegram(queries, limit_per_channel=30):
    """Собрать вакансии из настроенных каналов по списку запросов."""
    if not configured():
        return []

    from telethon import TelegramClient

    api_id = int(os.getenv("TG_API_ID"))
    api_hash = os.getenv("TG_API_HASH")
    session = os.getenv("TG_SESSION", "tg_reader")

    words = [q.lower() for q in queries if q]
    out = []

    client = TelegramClient(session, api_id, api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.warning(
                "Телеграм-источник не авторизован: нужен однократный вход "
                "по телефону, чтобы создать файл сессии"
            )
            return []
        for channel in channels():
            try:
                out += await _read_channel(client, channel, words, limit_per_channel)
            except Exception as exc:
                logger.warning("канал %s не прочитан: %s", channel, exc)
    finally:
        await client.disconnect()
    return out


async def _read_channel(client, channel, words, limit):
    found = []
    async for message in client.iter_messages(channel, limit=SCAN_LIMIT):
        text = message.message or ""
        if not text.strip():
            continue
        low = text.lower()
        if words and not any(w in low for w in words):
            continue
        if not _VACANCY_HINT.search(text) and not words:
            continue

        title = _title(text)
        if not title:
            continue

        published = message.date
        if published and published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)

        handle = channel.lstrip("@")
        salary_from, salary_to = _salary(text)
        found.append({
            "uid": f"tg:{handle}:{message.id}",
            "source": "tg",
            "ext_id": str(message.id),
            "title": title,
            "company": "",
            "area": "",
            "url": f"https://t.me/{handle}/{message.id}",
            "published_at": published.isoformat() if published else None,
            "salary_from": salary_from,
            "salary_to": salary_to,
            "currency": "RUR",
            "work_format": _work_format(text),
            "experience": "",
        })
        if len(found) >= limit:
            break
    return found
