"""
Сбор вакансий из открытых источников.

Источники:
  hh       - hh.ru. Официальное API с 2025 года требует токен приложения,
             поэтому по умолчанию берём данные из JSON-состояния страницы
             поиска (тот же ответ, что рисует выдачу в браузере).
             Если в окружении есть HH_TOKEN - работаем через api.hh.ru.
  habr     - career.habr.com, разбор карточек выдачи.
  getmatch - getmatch.ru, открытый JSON со свежими офферами.
  geekjob  - geekjob.ru, обход ленты (поиск по адресу сайт игнорирует).
  itone    - it-one.ru, вакансии одной компании.
  superjob - только через официальный API (SUPERJOB_KEY): обычные
             страницы поиска отдают капчу.
  tg       - телеграм-каналы (нужны api_id/api_hash, см. tg_source.py).

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

import tg_source

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

# Сколько страниц выдачи забирать с hh на каждый запрос: страница = 50
# самых свежих вакансий, одной страницы на широкие запросы не хватало.
HH_PAGES = int(os.getenv("HH_PAGES", "3"))


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


# Компании, которые интересны отдельно: их вакансии часто не попадают в общую
# выдачу (висят дольше окна поиска или названы по-своему). id работодателя на
# hh — из адреса hh.ru/employer/<id>.
HH_EMPLOYERS = {
    588914: "Aviasales",
    816144: "ВкусВилл",
    10317521: "Додо Пицца",
    1429999: "Циан",
    3530: "СДЭК",
    2324020: "Точка Банк",
    64174: "2ГИС",
    5063336: "Flowwow (Флаувау)",
    1316038: "Достависта",
    5775055: "Twinby",
    10745593: "GGSel",
    3536822: "Whoosh",
    678191: "Юрент",
    5987910: "Urent (Кикшеринг)",
}

# У работодателя может висеть тысяча вакансий (курьеры, продавцы), поэтому
# спрашиваем сразу с исследовательским запросом по названию.
EMPLOYER_QUERY = (
    "исследователь OR исследователи OR исследованиям OR исследований OR "
    "researcher OR research OR ресерчер OR ресёрч OR ресерч OR "
    "UX OR CX OR custdev OR юзабилити OR социолог"
)


async def fetch_hh_employer(client, employer_id, period=30):
    """Вакансии одной компании на hh — тем же разбором, что и обычный поиск."""
    try:
        resp = await _hh_get(
            client, "https://hh.ru/search/vacancy",
            {"text": EMPLOYER_QUERY, "employer_id": employer_id,
             "search_field": "name", "order_by": "publication_time",
             "search_period": period, "items_on_page": 50},
            {"User-Agent": UA},
        )
        if resp is None:
            return []
        m = re.search(
            r'<template[^>]*id="HH-Lux-InitialState"[^>]*>(.*?)</template>',
            resp.text, re.S)
        if not m:
            logger.warning("hh: не нашёл состояние страницы для компании %s", employer_id)
            return []
        result = json.loads(html_mod.unescape(m.group(1)))["vacancySearchResult"]
        return [_hh_item(i) for i in result.get("vacancies") or []]
    except Exception as exc:
        logger.warning("hh: компания %s не опросилась: %s %s",
                       employer_id, type(exc).__name__, exc)
        return []


# ------------------------------------------- скрытая удалёнка в описании

# В шапке вакансии стоит «гибрид» или «офис», а в тексте — «возможна
# удалёнка», «формат обсуждается», «офис по желанию». Такие вакансии
# помечаем форматом remote_maybe: «удалёнка по договорённости».
REMOTE_IN_TEXT = re.compile(
    r"возможн\w*\s+(?:\w+\s+){0,2}удал[ёе]нн"
    r"|удал[ёе]нк\w*\s+возможн"
    r"|можно\s+(?:работать\s+)?удал[ёе]нно"
    r"|удал[ёе]нн\w+\s+(?:работа|формат|график|режим|сотрудничеств|занятост)"
    r"|готовы\s+(?:обсуд|рассмотр)\w*\s+(?:\w+\s+){0,3}удал[ёе]нк"
    r"|формат\w*\s+(?:работы\s+)?(?:обсужда|гибк|на\s+выбор|по\s+договор|любой)"
    r"|(?:в\s+)?офисе?\s+или\s+удал[ёе]нно"
    r"|офис\s+по\s+желанию"
    r"|можно\s+из\s+дома"
    r"|из\s+люб(?:ой\s+точки|ого\s+города)"
    r"|work\s+from\s+anywhere"
    r"|remote\s+(?:is\s+)?possible"
    r"|(?:full[\s-]?)?remote\s+option",
    re.IGNORECASE)

# Рядом с теми же словами часто стоит отказ — «удалённая работа не
# предполагается». Такие совпадения не считаем.
REMOTE_DENIAL = re.compile(
    r"не\s+предполага|не\s+рассматрива|не\s+предусмотрен|без\s+удал[ёе]нк"
    r"|только\s+офис|исключительно\s+в\s+офисе|нет\s+удал[ёе]нк"
    r"|удал[ёе]нн\w+\s+работа\s+невозможна|не\s+удал[ёе]нн",
    re.IGNORECASE)

REMOTE_WINDOW = 90      # столько символов вокруг совпадения смотрим на отказ
ENRICH_LIMIT = 25       # сколько вакансий за цикл догружаем ради описания


def remote_mentioned(text):
    """Есть ли в описании обещание удалёнки, не перечёркнутое отказом."""
    plain = _strip_tags(text)
    for m in REMOTE_IN_TEXT.finditer(plain):
        around = plain[max(0, m.start() - REMOTE_WINDOW):m.end() + REMOTE_WINDOW]
        if not REMOTE_DENIAL.search(around):
            return True
    return False


async def fetch_hh_description(client, vacancy_id):
    resp = await _hh_get(client, "https://hh.ru/vacancy/" + str(vacancy_id),
                         {}, {"User-Agent": UA})
    if resp is None:
        return ""
    m = re.search(r'<template[^>]*id="HH-Lux-InitialState"[^>]*>(.*?)</template>',
                  resp.text, re.S)
    if not m:
        return ""
    view = json.loads(html_mod.unescape(m.group(1))).get("vacancyView") or {}
    return view.get("description") or ""


async def enrich_remote(items, limit=ENRICH_LIMIT):
    """Дочитывает описания вакансий, у которых в шапке удалёнки нет.

    Дорого (по запросу на вакансию), поэтому вызывается только для новых
    подходящих вакансий и не больше limit штук за цикл.
    """
    targets = [v for v in items
               if v.get("source") == "hh"
               and "remote" not in (v.get("work_format") or "")][:limit]
    if not targets:
        return 0

    found = 0
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for vac in targets:
            try:
                description = await fetch_hh_description(client, vac["ext_id"])
            except Exception as exc:
                logger.info("не смог прочитать описание %s: %s %s",
                            vac["uid"], type(exc).__name__, exc)
                continue
            if description and remote_mentioned(description):
                formats = [f for f in (vac.get("work_format") or "").split(",") if f]
                formats.append("remote_maybe")
                vac["work_format"] = ",".join(dict.fromkeys(formats))
                found += 1
    logger.info("описаний прочитано %s, скрытой удалёнки найдено %s",
                len(targets), found)
    return found


# --------------------------------------------------------------- Aviasales

# Свой сайт вакансий: React-приложение, но данные лежат в открытом JSON.
AVIASALES_API = "https://vacancies-app.aviasales.ru/api/vacancies"


async def fetch_aviasales(client):
    try:
        resp = await client.get(AVIASALES_API,
                                headers={"User-Agent": UA, "Accept": "application/json"})
        resp.raise_for_status()
        items = resp.json()
    except Exception as exc:
        logger.warning("aviasales: запрос не удался: %s %s", type(exc).__name__, exc)
        return []

    out = []
    for raw in items if isinstance(items, list) else []:
        vid = str(raw.get("id") or "")
        title = (raw.get("position") or "").strip()
        if not vid or not title:
            continue
        place = (raw.get("workPlace") or "").strip()
        out.append({
            "uid": "aviasales:" + vid,
            "source": "aviasales",
            "ext_id": vid,
            "title": title,
            "company": "Aviasales",
            "area": place,
            "url": "https://www.aviasales.ru/about/vacancies/" + vid,
            # даты в их API нет - считаем вакансию свежей с момента находки
            "published_at": _now_iso(),
            "salary_from": None,
            "salary_to": None,
            "currency": "",
            "work_format": "remote" if "удал" in place.lower() else "",
            "experience": "",
        })
    return out


# -------------------------------------------------------------- Dodo Brands

# Карьерный сайт на Nuxt, но данные лежат в открытом JSON: вакансии
# сгруппированы по направлениям, внутри каждого - список позиций.
DODO_API = "https://career-api.dodoteam.ru/api/v1/vacancies"
DODO_FORMATS = {"удал": "remote", "гибрид": "hybrid", "офис": "office"}


async def fetch_dodo(client):
    try:
        resp = await client.get(DODO_API,
                                headers={"User-Agent": UA, "Accept": "application/json"})
        resp.raise_for_status()
        groups = (resp.json() or {}).get("data") or []
    except Exception as exc:
        logger.warning("dodo: запрос не удался: %s %s", type(exc).__name__, exc)
        return []

    out = []
    for group in groups:
        for raw in group.get("items") or []:
            vid = str(raw.get("id") or "")
            title = (raw.get("position") or "").strip()
            if not vid or not title:
                continue
            formats = []
            for value in raw.get("work_format") or []:
                low = (value or "").lower()
                for key, code in DODO_FORMATS.items():
                    if key in low:
                        formats.append(code)
            out.append({
                "uid": "dodo:" + vid,
                "source": "dodo",
                "ext_id": vid,
                "title": title,
                "company": "Dodo Brands",
                "area": (raw.get("vacancy_location") or "").strip()[:60],
                "url": "https://dodoteam.ru/vacancy?vacancyId=" + vid,
                # даты в их API нет - считаем вакансию свежей с момента находки
                "published_at": _now_iso(),
                "salary_from": None,
                "salary_to": None,
                "currency": "",
                "work_format": ",".join(dict.fromkeys(formats)),
                "experience": "",
            })
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


# ----------------------------------------------------------------- LinkedIn

# Гостевая выдача LinkedIn: та же, что видна без входа в аккаунт.
LI_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
LI_LIMIT = asyncio.Semaphore(2)
LI_ID = re.compile(r'data-entity-urn="urn:li:jobPosting:(\d+)"')
LI_LINK = re.compile(r'base-card__full-link"[^>]*href="([^"?]+)')
LI_TITLE = re.compile(r'base-search-card__title"[^>]*>(.*?)</h3>', re.S)
LI_COMPANY = re.compile(r'base-search-card__subtitle"[^>]*>.*?<a[^>]*>(.*?)</a>', re.S)
LI_PLACE = re.compile(r'job-search-card__location"[^>]*>(.*?)</span>', re.S)
LI_DATE = re.compile(r'<time[^>]*datetime="([^"]+)"')


async def fetch_linkedin(client, query, location="Russian Federation",
                         period_days=30):
    # фильтр f_WT=2 («Remote») в гостевой выдаче не работает - она отдаёт то же
    # самое, поэтому удалёнку определяем по тексту названия и локации
    params = {"keywords": query, "location": location, "start": 0,
              "f_TPR": "r" + str(period_days * 86400)}
    try:
        async with LI_LIMIT:
            resp = await client.get(LI_URL, params=params, headers={"User-Agent": UA})
            await asyncio.sleep(1.0)
        if resp.status_code == 429:
            logger.info("linkedin: слишком часто, пропускаю запрос «%s»", query)
            return []
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("linkedin: запрос не удался (%s): %s %s",
                       query, type(exc).__name__, exc)
        return []

    out = []
    for chunk in resp.text.split("<li>")[1:]:
        m_id = LI_ID.search(chunk)
        m_title = LI_TITLE.search(chunk)
        if not m_id or not m_title:
            continue
        m_link = LI_LINK.search(chunk)
        m_company = LI_COMPANY.search(chunk)
        m_place = LI_PLACE.search(chunk)
        m_date = LI_DATE.search(chunk)
        out.append({
            "uid": "linkedin:" + m_id.group(1),
            "source": "linkedin",
            "ext_id": m_id.group(1),
            "title": _strip_tags(m_title.group(1)),
            "company": _strip_tags(m_company.group(1)) if m_company else "",
            "area": _strip_tags(m_place.group(1))[:60] if m_place else "",
            "url": m_link.group(1) if m_link
                   else "https://www.linkedin.com/jobs/view/" + m_id.group(1),
            "published_at": _to_iso(m_date.group(1) if m_date else None),
            "salary_from": None,
            "salary_to": None,
            "currency": "",
            "work_format": "",
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


# ----------------------------------------------------------------- geekjob

GEEKJOB_PAGES = 5
# Сколько вакансий догружаем ради даты публикации: в списке её нет.
GEEKJOB_DETAILS = 12
GEEKJOB_ITEM = re.compile(r'<li class="collection-item.*?</li>', re.S)
GEEKJOB_TITLE = re.compile(r'class="title"[^>]*>(.*?)</a>', re.S)
GEEKJOB_HREF = re.compile(r'href="(/vacancy/[0-9a-f]+)"')
GEEKJOB_COMPANY = re.compile(r'company-name"[^>]*>\s*<a[^>]*>(.*?)</a>', re.S)
GEEKJOB_SALARY = re.compile(r'class="salary">(.*?)</span>', re.S)
GEEKJOB_DATE = re.compile(r'"datePosted"\s*:\s*"([^"]+)"')


async def fetch_geekjob(client, queries):
    """geekjob.ru — вакансии в IT и digital.

    Поисковые параметры в адресе сайт игнорирует (на ?q=... приходит тот же
    общий список), поэтому обходим ленту и отбираем по названию сами.
    """
    words = [w.lower() for q in queries for w in re.split(r"\s+", q) if len(w) > 2]
    out, seen = [], set()

    for page in range(1, GEEKJOB_PAGES + 1):
        try:
            resp = await client.get("https://geekjob.ru/vacancies",
                                    params={"page": page}, headers={"User-Agent": UA})
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("geekjob: страница %s не открылась: %s", page, exc)
            break

        chunks = GEEKJOB_ITEM.findall(resp.text)
        if not chunks:
            break

        for chunk in chunks:
            href = GEEKJOB_HREF.search(chunk)
            title_m = GEEKJOB_TITLE.search(chunk)
            if not href or not title_m:
                continue
            title = _strip_tags(title_m.group(1))
            if words and not any(w in title.lower() for w in words):
                continue
            url = "https://geekjob.ru" + href.group(1)
            if url in seen:
                continue
            seen.add(url)

            salary_m = GEEKJOB_SALARY.search(chunk)
            salary_text = _strip_tags(salary_m.group(1)) if salary_m else ""
            company_m = GEEKJOB_COMPANY.search(chunk)
            out.append({
                "uid": "geekjob:" + href.group(1).rsplit("/", 1)[-1],
                "source": "geekjob",
                "ext_id": href.group(1).rsplit("/", 1)[-1],
                "title": title,
                "company": _strip_tags(company_m.group(1)) if company_m else "",
                "area": "",
                "url": url,
                "published_at": None,
                "salary_from": _num(salary_text) if salary_text else None,
                "salary_to": None,
                "currency": "RUR",
                "work_format": "",
                "experience": "",
            })

    for item in out[:GEEKJOB_DETAILS]:
        try:
            page = await client.get(item["url"], headers={"User-Agent": UA})
            m = GEEKJOB_DATE.search(page.text)
            if m:
                item["published_at"] = _to_iso(m.group(1))
        except Exception:
            pass  # дата приятна, но терять из-за неё вакансию не станем
    return out


# ------------------------------------------------------------------- IT_One

ITONE_TITLE = re.compile(r"<h3>(.*?)</h3>", re.S)
ITONE_HREF = re.compile(r'href="(/vacancies/[0-9a-f]+/)"')
ITONE_CITY = re.compile(r'class="city">(.*?)</span>', re.S)


async def fetch_itone(client, queries):
    """Вакансии IT_One (it-one.ru) — это одна компания, а не агрегатор.

    Дат публикации на сайте нет, поэтому такие вакансии проходят фильтр
    свежести как «без даты».
    """
    words = [w.lower() for q in queries for w in re.split(r"\s+", q) if len(w) > 2]
    try:
        resp = await client.get("https://www.it-one.ru/vacancies/",
                                headers={"User-Agent": UA})
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("it-one: страница не открылась: %s", exc)
        return []

    # Карточки режем по началу блока: внутри вложенные div, и подобрать
    # закрывающий тег регуляркой надёжно не выйдет.
    out = []
    for chunk in resp.text.split('<div class="element">')[1:]:
        title_m = ITONE_TITLE.search(chunk)
        href_m = ITONE_HREF.search(chunk)
        if not title_m or not href_m:
            continue
        title = _strip_tags(title_m.group(1))
        if words and not any(w in title.lower() for w in words):
            continue
        city_m = ITONE_CITY.search(chunk)
        city = _strip_tags(city_m.group(1)) if city_m else ""
        vid = href_m.group(1).strip("/").rsplit("/", 1)[-1]
        out.append({
            "uid": "itone:" + vid,
            "source": "itone",
            "ext_id": vid,
            "title": title,
            "company": "IT_One",
            "area": city,
            "url": "https://www.it-one.ru" + href_m.group(1),
            "published_at": None,
            "salary_from": None,
            "salary_to": None,
            "currency": "RUR",
            "work_format": "remote" if re.search(r"remote|удал", city, re.I) else "",
            "experience": "",
        })
    return out


# ----------------------------------------------------------------- superjob

SUPERJOB_KEY = os.getenv("SUPERJOB_KEY", "").strip()


async def fetch_superjob(client, query, period_days=30, count=100):
    """superjob.ru — только через официальный API.

    Обычные страницы поиска отдают капчу, обходить её мы не будем. Ключ
    бесплатный: регистрация приложения на https://api.superjob.ru/register
    и переменная окружения SUPERJOB_KEY.
    """
    if not SUPERJOB_KEY:
        return []
    try:
        resp = await client.get(
            "https://api.superjob.ru/2.0/vacancies/",
            params={"keyword": query, "count": count, "order_field": "date",
                    "period": period_days},
            headers={"X-Api-App-Id": SUPERJOB_KEY, "User-Agent": UA},
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("superjob: запрос не удался: %s", exc)
        return []

    out = []
    for raw in resp.json().get("objects", []):
        vid = str(raw.get("id"))
        place = (raw.get("town") or {}).get("title") or ""
        out.append({
            "uid": "superjob:" + vid,
            "source": "superjob",
            "ext_id": vid,
            "title": raw.get("profession") or "",
            "company": raw.get("firm_name") or "",
            "area": place,
            "url": raw.get("link") or "",
            "published_at": _to_iso(
                datetime.fromtimestamp(raw["date_published"], MSK).isoformat()
                if raw.get("date_published") else None
            ),
            "salary_from": raw.get("payment_from") or None,
            "salary_to": raw.get("payment_to") or None,
            "currency": (raw.get("currency") or "rub").upper().replace("RUB", "RUR"),
            "work_format": "remote" if raw.get("place_of_work", {}).get("id") == 2 else "",
            "experience": "",
        })
    return out


# -------------------------------------------------------------------- сборка

REMOTE_IN_TITLE = re.compile(
    r"удал[её]нн|удал[её]нка|дистанцион|\bremote\b|work\s*from\s*home", re.IGNORECASE)


# Вакансии старше этого срока не показываем.
MAX_AGE_DAYS = 92

# Мусор в названии, который мешает опознать одну и ту же вакансию на разных
# сайтах: уточнения в скобках, грейд, слово «вакансия».
_TITLE_NOISE = re.compile(r"\(.*?\)|\[.*?\]|\bвакансия\b", re.I)
_GRADE = re.compile(
    r"\b(junior|middle|senior|lead|jun|mid|sr|jr|стажер|стажёр|младший|старший|ведущий)\b",
    re.I,
)
_ORG_PREFIX = re.compile(r"\b(ооо|оао|зао|пао|ип|llc|ltd|inc|gmbh|corp|компания)\b", re.I)


def _norm(text, drop_grade=False):
    t = (text or "").lower().replace("ё", "е")
    t = _TITLE_NOISE.sub(" ", t)
    if drop_grade:
        t = _GRADE.sub(" ", t)
    else:
        t = _ORG_PREFIX.sub(" ", t)
    t = re.sub(r"[^a-zа-я0-9+#. ]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def merge_key(item):
    """Ключ, по которому вакансия считается той же самой на разных сайтах."""
    return _norm(item.get("title"), drop_grade=True) + "|" + _norm(item.get("company"))


def is_fresh(item, max_age_days=MAX_AGE_DAYS, now=None):
    """Не архивная и не старше max_age_days.

    Вакансии без даты публикации оставляем: её не отдают телеграм-каналы,
    и молча выбрасывать их значило бы терять часть выдачи.
    """
    if item.get("archived"):
        return False
    published = item.get("published_at")
    if not published:
        return True
    try:
        dt = datetime.fromisoformat(published)
    except ValueError:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = ((now or datetime.now(timezone.utc)) - dt).total_seconds() / 86400
    return age <= max_age_days


def merge_duplicates(items):
    """Склеить одну и ту же вакансию, найденную в разных сервисах.

    Раньше дубли убирались только по uid, а он начинается с имени источника
    («hh:123», «habr:456»), поэтому одна вакансия на hh и на Хабре приходила
    дважды. Теперь такие карточки объединяются, а ссылки на все сервисы,
    где вакансия нашлась, собираются в links.
    """
    merged, order = {}, []
    for item in items:
        item.setdefault("links", [(item.get("source", ""), item.get("url", ""))])
        key = merge_key(item)
        base = merged.get(key)
        if base is None:
            merged[key] = item
            order.append(key)
            continue

        known = {url for _, url in base["links"]}
        for src, url in item["links"]:
            if url and url not in known:
                base["links"].append((src, url))
                known.add(url)

        # Недостающие поля добираем из дубля: на одном сайте может быть
        # указана зарплата или формат работы, а на другом нет.
        for field in ("salary_from", "salary_to", "currency", "experience", "area", "company"):
            if not base.get(field) and item.get(field):
                base[field] = item[field]
        formats = dict.fromkeys(
            [f for f in (base.get("work_format") or "").split(",") if f]
            + [f for f in (item.get("work_format") or "").split(",") if f]
        )
        base["work_format"] = ",".join(formats)
        if item.get("published_at") and (
            not base.get("published_at") or item["published_at"] < base["published_at"]
        ):
            base["published_at"] = item["published_at"]
    return [merged[k] for k in order]


def _augment_formats(item):
    """Если «удалённо» написано в названии или в локации, а формата нет.

    Локация нужна для LinkedIn: там формат работы отдельным полем не приходит,
    зато в месте работы пишут «Moscow, Russia (Remote)».
    """
    formats = [f for f in (item.get("work_format") or "").split(",") if f]
    where = (item.get("title") or "") + " " + (item.get("area") or "")
    if REMOTE_IN_TITLE.search(where) and "remote" not in formats:
        formats.append("remote")
    item["work_format"] = ",".join(formats)
    return item


async def fetch_all(sources, hh_queries, habr_queries, area=113, period=7):
    """Опрашивает включённые источники и возвращает вакансии без дублей."""
    tasks = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        if "hh" in sources:
            for q in hh_queries:
                # Несколько страниц: hh отдаёт по 50 самых свежих на страницу,
                # и на широких запросах всё остальное просто терялось.
                tasks.append(fetch_hh(client, q, area=area, period=period, pages=HH_PAGES))
                # отдельный проход по удалёнке: широкие запросы иначе
                # обрезаются по 50 самых свежих, и remote в них не попадает
                tasks.append(fetch_hh(client, q, area=area, period=period,
                                      pages=HH_PAGES, remote=True))
        if "habr" in sources:
            tasks += [fetch_habr(client, q) for q in habr_queries]
            tasks += [fetch_habr(client, q, remote=True) for q in habr_queries]
        if "getmatch" in sources:
            tasks.append(fetch_getmatch(client))
        if "geekjob" in sources:
            tasks.append(fetch_geekjob(client, habr_queries))
        if "itone" in sources:
            tasks.append(fetch_itone(client, habr_queries))
        if "superjob" in sources and SUPERJOB_KEY:
            tasks += [fetch_superjob(client, q) for q in habr_queries]
        if "tg" in sources and tg_source.configured():
            tasks.append(tg_source.fetch_telegram(client, habr_queries))
        if "linkedin" in sources:
            tasks += [fetch_linkedin(client, q, period_days=period)
                      for q in habr_queries]
        if "companies" in sources:
            tasks += [fetch_hh_employer(client, eid, period=period)
                      for eid in HH_EMPLOYERS]
            tasks.append(fetch_aviasales(client))
            tasks.append(fetch_dodo(client))
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

    out = merge_duplicates([v for v in out if is_fresh(v)])
    out.sort(key=lambda v: v.get("published_at") or "", reverse=True)
    return out


async def free_search(text, area=113, period=30, limit=10):
    """Разовый поиск по произвольному запросу - для команды /search.

    Идёт сразу по нескольким источникам, чтобы одна вакансия, висящая
    на hh и на Хабре, пришла одной карточкой со ссылками на оба.
    """
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        results = await asyncio.gather(
            fetch_hh(client, text, area=area, period=period),
            fetch_habr(client, text),
            return_exceptions=True,
        )

    items = []
    for res in results:
        if isinstance(res, Exception):
            logger.warning("источник упал при свободном поиске: %s", res)
            continue
        items += res

    items = merge_duplicates([_augment_formats(v) for v in items if is_fresh(v)])
    items.sort(key=lambda v: v.get("published_at") or "", reverse=True)
    return items[:limit]
