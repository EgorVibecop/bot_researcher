"""
Сбор вакансий из открытых источников.

Источники:
  hh       - hh.ru. Официальное API с 2025 года требует токен приложения,
             поэтому по умолчанию берём данные из JSON-состояния страницы
             поиска (тот же ответ, что рисует выдачу в браузере).
             Если в окружении есть HH_TOKEN - работаем через api.hh.ru.
  habr     - career.habr.com, разбор карточек выдачи.
  getmatch - getmatch.ru, открытый JSON со свежими офферами.

Каждый источник возвращает список словарей одного вида:
  uid, source, ext_id, title, company, area, url, published_at (ISO),
  salary_from, salary_to, currency, work_format, experience
"""

import asyncio
import html as html_mod
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import httpx

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HH_TOKEN = os.getenv("HH_TOKEN", "").strip()

HH_EXPERIENCE = {
    "noExperience": "без опыта",
    "between1And3": "1-3 года",
    "between3And6": "3-6 лет",
    "moreThan6": "6+ лет",
    "doesNotMatter": "",
}
HH_WORK_FORMAT = {"REMOTE": "remote", "HYBRID": "hybrid", "ON_SITE": "office"}

# hh отдаёт 429, если долбить его десятком параллельных запросов
HH_LIMIT = asyncio.Semaphore(2)
HH_PAUSE = 0.7


async def _hh_get(client, url, params, headers):
    """Запрос к hh с очередью и одной повторной попыткой после 429."""
    for attempt in range(2):
        async with HH_LIMIT:
            resp = await client.get(url, params=params, headers=headers)
            await asyncio.sleep(HH_PAUSE)
        if resp.status_code == 429 and attempt == 0:
            logger.info("hh просит подождать, повторю через 5 секунд")
            await asyncio.sleep(5)
            continue
        resp.raise_for_status()
        return resp
    return None


def _now_iso():
    return datetime.now(MSK).isoformat(timespec="seconds")


def _to_iso(value):
    """Приводит дату источника к ISO по Москве. При неудаче - текущее время."""
    if not value:
        return _now_iso()
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return _now_iso()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK).isoformat(timespec="seconds")


def _num(text):
    """'200 000 rub' -> 200000, иначе None."""
    digits = re.sub(r"[^\d]", "", (text or "").replace("&nbsp;", " "))
    return int(digits) if digits else None


def _strip_tags(chunk):
    text = re.sub(r"<[^>]+>", " ", chunk or "")
    return re.sub(r"\s+", " ", html_mod.unescape(text)).strip()


# --------------------------------------------------------------------- hh.ru

def _hh_item(raw):
    comp = raw.get("compensation") or {}
    # у вакансии может быть сразу несколько форматов: офис + удалённо + гибрид
    codes = [code for block in (raw.get("workFormats") or [])
             for code in (block.get("workFormatsElement") or [])]
    formats = [HH_WORK_FORMAT[c] for c in codes if c in HH_WORK_FORMAT]
    if raw.get("@workSchedule") == "remote":
        formats.append("remote")
    fmt = ",".join(dict.fromkeys(formats))
    links = raw.get("links") or {}
    vid = str(raw.get("vacancyId"))
    return {
        "uid": "hh:" + vid,
        "source": "hh",
        "ext_id": vid,
        "title": raw.get("name") or "",
        "company": (raw.get("company") or {}).get("visibleName")
        or (raw.get("company") or {}).get("name") or "",
        "area": (raw.get("area") or {}).get("name") or "",
        "url": links.get("desktop") or ("https://hh.ru/vacancy/" + vid),
        "published_at": _to_iso((raw.get("publicationTime") or {}).get("$")),
        "salary_from": comp.get("from"),
        "salary_to": comp.get("to"),
        "currency": comp.get("currencyCode") or "",
        "work_format": fmt,
        "experience": HH_EXPERIENCE.get(raw.get("workExperience"), ""),
    }


def _hh_api_item(raw):
    salary = raw.get("salary") or {}
    schedule = (raw.get("schedule") or {}).get("id") or ""
    vid = str(raw.get("id"))
    return {
        "uid": "hh:" + vid,
        "source": "hh",
        "ext_id": vid,
        "title": raw.get("name") or "",
        "company": (raw.get("employer") or {}).get("name") or "",
        "area": (raw.get("area") or {}).get("name") or "",
        "url": raw.get("alternate_url") or ("https://hh.ru/vacancy/" + vid),
        "published_at": _to_iso(raw.get("published_at")),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "currency": salary.get("currency") or "",
        "work_format": "remote" if schedule == "remote" else "",
        "experience": HH_EXPERIENCE.get((raw.get("experience") or {}).get("id"), ""),
    }


async def fetch_hh(client, query, area=113, period=7, pages=1, remote=False):
    out = []
    for page in range(pages):
        try:
            if HH_TOKEN:
                params = {"text": query, "area": area, "search_field": "name",
                          "order_by": "publication_time", "period": period,
                          "per_page": 50, "page": page}
                if remote:
                    params["schedule"] = "remote"
                resp = await _hh_get(
                    client, "https://api.hh.ru/vacancies", params,
                    {"Authorization": "Bearer " + HH_TOKEN, "User-Agent": "JobRadar/1.0"})
                if resp is None:
                    break
                items = resp.json().get("items") or []
                out.extend(_hh_api_item(i) for i in items)
                if len(items) < 50:
                    break
                continue

            web_params = {"text": query, "area": area, "search_field": "name",
                          "order_by": "publication_time", "search_period": period,
                          "items_on_page": 50, "page": page}
            if remote:
                web_params["work_format"] = "REMOTE"
            resp = await _hh_get(client, "https://hh.ru/search/vacancy",
                                 web_params, {"User-Agent": UA})
            if resp is None:
                break
            m = re.search(
                r'<template[^>]*id="HH-Lux-InitialState"[^>]*>(.*?)</template>',
                resp.text, re.S,
            )
            if not m:
                logger.warning("hh: не нашёл состояние страницы (запрос: %s)", query[:40])
                break
            result = json.loads(html_mod.unescape(m.group(1)))["vacancySearchResult"]
            items = result.get("vacancies") or []
            out.extend(_hh_item(i) for i in items)
            if len(items) < 50:
                break
        except Exception as exc:
            logger.warning("hh: запрос не удался (%s): %s", query[:40], exc)
            break
    return out


# ------------------------------------------------------------- Хабр Карьера

HABR_ID = re.compile(r'href="/vacancies/(\d+)"')
HABR_DATE = re.compile(r'<time[^>]*datetime="([^"]+)"')
HABR_COMPANY = re.compile(r'vacancy-card__company[^>]*>\s*<a[^>]*>(.*?)</a>', re.S)
HABR_TITLE = re.compile(r'vacancy-card__title-link"[^>]*>(.*?)</a>', re.S)
HABR_SALARY = re.compile(r'vacancy-card__salary"[^>]*>(.*?)</div>\s*<div', re.S)
HABR_CITY = re.compile(
    r'#placemark[^"]*"[^>]*>.*?chip-with-icon__text"[^>]*>([^<]*)', re.S)
HABR_REMOTE = re.compile(r"можно\s+удал[её]нно", re.IGNORECASE)


def _habr_salary(chunk):
    text = _strip_tags(chunk)
    if not text or "не указана" in text:
        return None, None, ""
    text = text.replace(" ", " ").replace(" ", " ")
    currency = "RUR" if "₽" in text else ("USD" if "$" in text else "")
    m = re.search(r"от\s*([\d\s]+).*?до\s*([\d\s]+)", text)
    if m:
        return _num(m.group(1)), _num(m.group(2)), currency
    m = re.search(r"от\s*([\d\s]+)", text)
    if m:
        return _num(m.group(1)), None, currency
    m = re.search(r"до\s*([\d\s]+)", text)
    if m:
        return None, _num(m.group(1)), currency
    m = re.search(r"([\d\s]{4,})", text)
    if m:
        return _num(m.group(1)), None, currency
    return None, None, ""


async def fetch_habr(client, query, remote=False):
    params = {"q": query, "type": "all", "sort": "date"}
    if remote:
        params["remote"] = "true"
    try:
        resp = await client.get(
            "https://career.habr.com/vacancies",
            params=params,
            headers={"User-Agent": UA},
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("habr: запрос не удался (%s): %s", query, exc)
        return []

    out = []
    for chunk in resp.text.split('<div class="vacancy-card">')[1:]:
        m_id = HABR_ID.search(chunk)
        m_title = HABR_TITLE.search(chunk)
        if not m_id or not m_title:
            continue
        m_sal = HABR_SALARY.search(chunk)
        s_from, s_to, currency = _habr_salary(m_sal.group(1) if m_sal else "")
        # в мете вперемешку город, грейд и «Можно удалённо»;
        # город помечен иконкой placemark, остальное нам не нужно
        remote = bool(HABR_REMOTE.search(chunk))
        m_city = HABR_CITY.search(chunk)
        city = _strip_tags(m_city.group(1))[:60] if m_city else ""
        m_company = HABR_COMPANY.search(chunk)
        m_date = HABR_DATE.search(chunk)
        out.append({
            "uid": "habr:" + m_id.group(1),
            "source": "habr",
            "ext_id": m_id.group(1),
            "title": _strip_tags(m_title.group(1)),
            "company": _strip_tags(m_company.group(1)) if m_company else "",
            "area": city,
            "url": "https://career.habr.com/vacancies/" + m_id.group(1),
            "published_at": _to_iso(m_date.group(1) if m_date else None),
            "salary_from": s_from,
            "salary_to": s_to,
            "currency": currency,
            "work_format": "remote" if remote else "office",
            "experience": "",
        })
    return out


# ----------------------------------------------------------------- getmatch

async def fetch_getmatch(client, limit=100, pages=2):
    out = []
    for page in range(pages):
        try:
            resp = await client.get(
                "https://getmatch.ru/api/offers",
                params={"limit": limit, "offset": page * limit,
                        "sort": "published_at"},
                headers={"User-Agent": UA, "Accept": "application/json"},
            )
            resp.raise_for_status()
            offers = resp.json().get("offers") or []
        except Exception as exc:
            logger.warning("getmatch: запрос не удался: %s", exc)
            break
        for o in offers:
            if not o.get("is_active"):
                continue
            oid = str(o.get("id"))
            places = [p for p in (o.get("location_items") or []) if isinstance(p, dict)]
            locations = ", ".join(p.get("label") or "" for p in places)[:60]
            formats = []
            for place in places:
                code = place.get("format") or ""
                formats.append(code if code in ("remote", "hybrid", "office") else "office")
            work_format = ",".join(dict.fromkeys(formats))
            url = o.get("url") or ""
            if url.startswith("/"):
                url = "https://getmatch.ru" + url
            out.append({
                "uid": "getmatch:" + oid,
                "source": "getmatch",
                "ext_id": oid,
                "title": o.get("position") or "",
                "company": (o.get("company") or {}).get("name") or "",
                "area": locations,
                "url": url or ("https://getmatch.ru/vacancies/" + oid),
                "published_at": _to_iso(o.get("published_at")),
                "salary_from": o.get("salary_display_from"),
                "salary_to": o.get("salary_display_to"),
                "currency": (o.get("salary_currency") or "").upper(),
                "work_format": work_format,
                "experience": "",
            })
        if len(offers) < limit:
            break
    return out


# -------------------------------------------------------------------- сборка

REMOTE_IN_TITLE = re.compile(
    r"удал[её]нн|удал[её]нка|дистанцион|\bremote\b|work\s*from\s*home", re.IGNORECASE)


def _augment_formats(item):
    """Если в названии написано «удалённо», а источник формат не отдал."""
    formats = [f for f in (item.get("work_format") or "").split(",") if f]
    if REMOTE_IN_TITLE.search(item.get("title") or "") and "remote" not in formats:
        formats.append("remote")
    item["work_format"] = ",".join(formats)
    return item


async def fetch_all(sources, hh_queries, habr_queries, area=113, period=7):
    """Опрашивает включённые источники и возвращает вакансии без дублей."""
    tasks = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        if "hh" in sources:
            for q in hh_queries:
                tasks.append(fetch_hh(client, q, area=area, period=period))
                # отдельный проход по удалёнке: широкие запросы иначе
                # обрезаются по 50 самых свежих, и remote в них не попадает
                tasks.append(fetch_hh(client, q, area=area, period=period, remote=True))
        if "habr" in sources:
            tasks += [fetch_habr(client, q) for q in habr_queries]
            tasks += [fetch_habr(client, q, remote=True) for q in habr_queries]
        if "getmatch" in sources:
            tasks.append(fetch_getmatch(client))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    seen, out = {}, []
    for res in results:
        if isinstance(res, Exception):
            logger.warning("источник упал: %s", res)
            continue
        for item in res:
            if not item["title"]:
                continue
            known = seen.get(item["uid"])
            if known is not None:
                # одна и та же вакансия из обычного и «удалённого» прохода:
                # склеиваем форматы работы
                formats = dict.fromkeys(
                    [f for f in (known["work_format"] or "").split(",") if f]
                    + [f for f in (item["work_format"] or "").split(",") if f])
                known["work_format"] = ",".join(formats)
                continue
            seen[item["uid"]] = item
            out.append(_augment_formats(item))
    out.sort(key=lambda v: v["published_at"], reverse=True)
    return out


async def free_search(text, area=113, period=30, limit=10):
    """Разовый поиск по произвольному запросу - для команды /search."""
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        items = await fetch_hh(client, text, area=area, period=period)
    return items[:limit]
