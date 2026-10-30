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
import hashlib
import html as html_mod
import json
import logging
import os
import re
from urllib.parse import quote
from datetime import datetime, timedelta, timezone

import httpx

import db
import matching
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
# Страницы hh по 50 вакансий. Листаем, пока выдача не кончится или пока
# вакансии не станут старше окна поиска: фиксированный потолок обрезал
# широкие запросы на первой же неделе. 40 - предел самого hh (2000 вакансий
# на запрос), дальше он всё равно ничего не отдаёт.
HH_PAGES = int(os.getenv("HH_PAGES", "40"))
# Глубина обхода остальных площадок: везде останавливаемся на первой
# неполной странице, так что это лишь предохранитель.
HABR_PAGES = int(os.getenv("HABR_PAGES", "5"))
# У Работы.ру разметка с вакансиями есть только на первой странице:
# вторая отдаётся пустым каркасом, листать её бессмысленно.
RABOTA_PAGES = int(os.getenv("RABOTA_PAGES", "1"))
TRUDVSEM_PAGES = int(os.getenv("TRUDVSEM_PAGES", "5"))
GETMATCH_PAGES = int(os.getenv("GETMATCH_PAGES", "5"))


async def _hh_get(client, url, params, headers):
    """Запрос к hh с очередью и повторами.

    Повторяем не только 429, но и сетевой сбой с ответами 5xx: сбой на
    третьей странице обрывал обход запроса целиком, и всё, что лежало
    дальше, в этот цикл не попадало.
    """
    for attempt in range(3):
        try:
            async with HH_LIMIT:
                resp = await client.get(url, params=params, headers=headers)
                await asyncio.sleep(HH_PAUSE)
        except Exception as exc:
            if attempt == 2:
                raise
            logger.info("hh: сбой связи (%s), повторю", type(exc).__name__)
            await asyncio.sleep(3)
            continue
        if attempt < 2 and (resp.status_code == 429 or resp.status_code >= 500):
            logger.info("hh ответил %s, повторю", resp.status_code)
            await asyncio.sleep(5 if resp.status_code == 429 else 3)
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
    # Сначала выбрасываем скрипты, стили и шаблоны целиком: иначе в текст
    # попадает JSON страницы, и бот читает служебные строки как настоящие.
    # У hh в <template> лежит словарь переводов интерфейса - там есть и
    # «Вакансия закрыта», из-за чего живые вакансии считались архивными.
    text = re.sub(r"<(script|style|template)\b[^>]*>[\s\S]*?</\1>", " ",
                  chunk or "", flags=re.IGNORECASE)
    # Остатки экранированного JSON («&#34;ключ&#34;:&#34;значение&#34;»)
    # тоже не текст страницы.
    text = re.sub(r"&#34;[^<]{0,400}?&#34;\s*:\s*&#34;[^<]{0,400}?&#34;", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
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
    # hh сам отмечает аккредитованных работодателей - это официальный
    # реестр Минцифры, точнее любых слов в описании.
    accredited = (raw.get("company") or {}).get("accreditedITEmployer")
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
        "it_accred": "yes" if accredited else ("no" if accredited is False else ""),
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


def _older_than(published_at, days):
    """Опубликована раньше, чем окно поиска? Без даты считаем, что нет."""
    if not published_at:
        return False
    try:
        stamp = datetime.fromisoformat(published_at)
    except Exception:
        return False
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=MSK)
    return stamp < datetime.now(MSK) - timedelta(days=days)


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
            parsed = [_hh_item(i) for i in items]
            out.extend(parsed)
            if len(items) < 50:
                break
            # Выдача отсортирована от свежих к старым: как только страница
            # целиком ушла за окно поиска, дальше листать незачем.
            if parsed and all(_older_than(v.get("published_at"), period)
                              for v in parsed):
                break
        except Exception as exc:
            logger.warning("hh: запрос не удался (%s): %s", query[:40], exc)
            break
    return out


# --------------------------------------------------- Работа России (ГосТруд)

# Государственный портал trudvsem.ru с открытым API: почти полмиллиона
# вакансий, ключ не нужен. Туда обязаны выкладывать вакансии госкомпании и
# многие крупные работодатели, так что пересечение с hh неполное.
TRUDVSEM_API = "https://opendata.trudvsem.ru/api/v1/vacancies"


async def fetch_trudvsem(client, query, limit=100, pages=TRUDVSEM_PAGES):
    out, seen = [], set()
    for page in range(pages):
        got = await _trudvsem_page(client, query, limit, page)
        fresh = [v for v in got if v["uid"] not in seen]
        if not fresh:
            break
        seen.update(v["uid"] for v in fresh)
        out.extend(fresh)
        if len(got) < limit:
            break
    return out


async def _trudvsem_page(client, query, limit, page):
    try:
        resp = await client.get(TRUDVSEM_API,
                                params={"text": query, "limit": limit, "offset": page},
                                headers={"User-Agent": UA, "Accept": "application/json"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("trudvsem: запрос не удался (%s, стр. %s): %s %s",
                       query[:30], page, type(exc).__name__, exc)
        return []

    out = []
    for row in (data.get("results") or {}).get("vacancies") or []:
        raw = row.get("vacancy") or {}
        vid = str(raw.get("id") or "")
        title = (raw.get("job-name") or "").strip()
        if not vid or not title:
            continue
        # Формат работы они пишут словами в двух разных полях.
        where = " ".join(str(raw.get(f) or "")
                         for f in ("schedule", "workPlaceType", "conditions"))
        formats = []
        if re.search(r"дистанцион|удал[её]нн|remote", where, re.I):
            formats.append("remote")
        if re.search(r"гибрид", where, re.I):
            formats.append("hybrid")
        company = (raw.get("company") or {}).get("name") or ""
        out.append({
            "uid": "trudvsem:" + vid,
            "source": "trudvsem",
            "ext_id": vid,
            "title": title,
            "company": company.strip(),
            "area": (raw.get("region") or {}).get("name") or "",
            "url": raw.get("vac_url") or "",
            "published_at": _to_iso(raw.get("creation-date")),
            "salary_from": raw.get("salary_min") or None,
            "salary_to": raw.get("salary_max") or None,
            "currency": raw.get("currency") or "RUR",
            "work_format": ",".join(dict.fromkeys(formats)),
            "experience": "",
        })
    return out


# ------------------------------------------------------------- rabota.ru

# Крупный агрегатор, не только IT. Вакансии отдаёт разметкой schema.org,
# поэтому разбираем тем же кодом, что и карьерные сайты.
async def fetch_rabota(client, query, pages=RABOTA_PAGES):
    out, seen = [], set()
    for page in range(1, pages + 1):
        fresh = [v for v in await _rabota_page(client, query, page)
                 if v["uid"] not in seen]
        if not fresh:
            break
        seen.update(v["uid"] for v in fresh)
        out.extend(fresh)
    return out


async def _rabota_page(client, query, page):
    try:
        resp = await client.get("https://www.rabota.ru/vacancy/",
                                params={"query": query, "page": page},
                                headers={"User-Agent": UA})
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("rabota.ru: запрос не удался (%s, стр. %s): %s %s",
                       query[:30], page, type(exc).__name__, exc)
        return []

    out = []
    for raw in _ld_jobs(resp.text):
        url = raw["url"]
        if not raw["title"] or not url:
            continue
        vid = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
        out.append({
            "uid": "rabota:" + vid,
            "source": "rabota",
            "ext_id": vid,
            "title": raw["title"],
            "company": "",
            "area": raw["area"],
            "url": url,
            "published_at": raw["published_at"],
            "salary_from": None,
            "salary_to": None,
            "currency": "RUR",
            "work_format": "",
            "experience": "",
        })
    return out


# ------------------------------------------------------------ zarplata.ru

# Сайт из той же семьи, что и hh: страница отдаёт то же состояние
# HH-Lux-InitialState, поэтому разбор берём готовый. Пул вакансий при этом
# свой - больше регионов и компаний, которых на hh нет.
ZP_SEARCH = "https://www.zarplata.ru/search/vacancy"


async def fetch_zarplata(client, query, area=113, period=7, pages=1):
    out = []
    for page in range(pages):
        try:
            resp = await _hh_get(client, ZP_SEARCH,
                                 {"text": query, "area": area, "search_field": "name",
                                  "order_by": "publication_time", "search_period": period,
                                  "items_on_page": 50, "page": page},
                                 {"User-Agent": UA})
            if resp is None:
                break
            m = re.search(
                r'<template[^>]*id="HH-Lux-InitialState"[^>]*>(.*?)</template>',
                resp.text, re.S)
            if not m:
                logger.warning("zarplata: не нашёл состояние страницы (%s)", query[:40])
                break
            items = (json.loads(html_mod.unescape(m.group(1)))
                     .get("vacancySearchResult") or {}).get("vacancies") or []
            parsed = []
            for raw in items:
                vac = _hh_item(raw)
                vac["uid"] = "zp:" + vac["ext_id"]
                vac["source"] = "zarplata"
                parsed.append(vac)
            out.extend(parsed)
            if len(items) < 50:
                break
            if parsed and all(_older_than(v.get("published_at"), period)
                              for v in parsed):
                break
        except Exception as exc:
            logger.warning("zarplata: запрос не удался (%s): %s", query[:40], exc)
            break
    return out


# Компании, которые интересны отдельно: их вакансии часто не попадают в общую
# выдачу (висят дольше окна поиска или названы по-своему). id работодателя на
# hh — из адреса hh.ru/employer/<id>.
HH_EMPLOYERS = {
    588914: "Aviasales",
    776314: "Золотое Яблоко",
    2180: "Ozon",
    78638: "Т-Банк",
    87021: "Wildberries",
    9352463: "X5 Tech",
    4233: "Х5",
    1740: "Яндекс",
    780654: "Lamoda",
    # Второй аккаунт Lamoda: там свои вакансии, а не копии первого
    # (job.lamoda.ru список рисует скриптом, разбирать нечего).
    9418714: "Lamoda Tech",
    # profi.ru/vacancies - приложение на Next.js, карточек в разметке нет
    83639: "Профи.ру",
    633069: "Selectel",
    41862: "Контур",
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
    # Компании, чей карьерный сайт закрыт для нас блокировками или собран
    # на JS: их страница работодателя на hh - единственный надёжный путь.
    # Если сайт всё-таки откроется, карточка с него станет основной при
    # склейке дублей, а эта останется второй ссылкой.
    158720: "Hoff",
    1122462: "Skyeng",
    1096484: "Tripster",
    9329959: "Orion soft",
    793926: "YCLIENTS",
    8582: "Лемана ПРО",
    49357: "Магнит",
    # Свой HR-API AliExpress (aero.aliexpress-inc.ru) с сервера не
    # отвечает вовсе - ConnectTimeout, как у Додо: хост закрыт для
    # дата-центров. Сборщик fetch_aliexpress оставлен на случай другого
    # хостинга, а реальный путь к их вакансиям - страница на hh.
    2592107: "AliExpress",
}

# У работодателя может висеть тысяча вакансий (курьеры, продавцы), поэтому
# спрашиваем сразу с исследовательским запросом по названию.
# Сам запрос зависит от профиля: исследователь или аналитик.
EMPLOYER_QUERY = matching.EMPLOYER_QUERY


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


# ------------------------------------------------- проверка «жива ли ещё»

# Источники, которые отдают полный каталог вакансий. Если вакансии этого
# источника нет в свежей выдаче - значит её сняли. Для поисковых выдач
# (hh, Хабр, LinkedIn) так рассуждать нельзя: там вакансия выпадает из
# ответа просто потому, что не подошла под запрос.
FULL_CATALOG_SOURCES = {"getmatch", "avito", "2gis", "dodo", "aviasales"}

# Как выглядит снятая вакансия на странице
CLOSED_MARKERS = re.compile(
    r"вакансия\s+в\s+архиве|вакансия\s+закрыта|вакансия\s+удалена"
    r"|больше\s+не\s+актуальн|закрыт\w+\s+ваканс'?и"
    r"|no\s+longer\s+accepting\s+applications|this\s+job\s+is\s+no\s+longer"
    r"|вакансия\s+не\s+найдена|offer\s+is\s+not\s+active"
    r"|вакансия\s+(?:уже\s+)?(?:неактуальн|снята)"
    r"|публикация\s+вакансии\s+(?:завершена|остановлена)"
    r"|этой\s+вакансии\s+(?:больше\s+)?нет"
    r"|position\s+(?:has\s+been\s+)?(?:closed|filled)",
    re.IGNORECASE)


async def _hh_archived(client, vac):
    """Спрашивает у API hh, в архиве ли вакансия: True/False, None - не знаю.

    Точнее, чем поиск слов на странице: hh отвечает хостингу то 403, то
    капчей, и тогда проверка по тексту считала живыми уже снятые вакансии.
    """
    if vac.get("source") != "hh":
        return None
    ext = str(vac.get("ext_id") or "")
    if not ext.isdigit():
        return None
    headers = {"User-Agent": "JobRadar/1.0"}
    if HH_TOKEN:
        headers["Authorization"] = "Bearer " + HH_TOKEN
    try:
        resp = await client.get("https://api.hh.ru/vacancies/" + ext,
                                headers=headers, timeout=15)
    except Exception:
        return None
    if resp.status_code in (404, 410):
        return True
    if resp.status_code != 200:
        return None
    try:
        return bool(resp.json().get("archived"))
    except Exception:
        return None


async def is_alive(client, vac):
    """Открыта ли ещё вакансия. При любой неясности считаем, что открыта:
    лучше показать лишнее, чем молча проглотить живую вакансию."""
    archived = await _hh_archived(client, vac)
    if archived is not None:
        return not archived
    url = vac.get("url")
    if not url:
        return True
    try:
        resp = await client.get(url, headers={"User-Agent": UA})
    except Exception as exc:
        logger.info("не смог проверить %s: %s", vac.get("uid"), type(exc).__name__)
        return True
    if resp.status_code in (404, 410):
        return False
    if resp.status_code != 200:
        return True
    # Ищем признак архива только в видимом тексте: в коде страницы у hh
    # лежит словарь интерфейса, где слова «вакансия закрыта» есть всегда.
    return not CLOSED_MARKERS.search(_strip_tags(resp.text[:600000]))


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
    r"|формат\w*\s+(?:работы\s+)?(?:обсужда|на\s+выбор|по\s+договор)"
    r"|(?:в\s+)?офисе?\s+или\s+удал[ёе]нно"
    r"|офис\s+по\s+желанию"
    r"|можно\s+из\s+дома"
    r"|из\s+люб(?:ой\s+точки|ого\s+города)"
    r"|work\s+from\s+anywhere"
    r"|remote\s+(?:is\s+)?possible"
    # английские формулировки - нужны для LinkedIn, он формат работы
    # в выдаче не отдаёт, зато пишет его на странице вакансии
    r"|workplace\s*type[^.]{0,40}remote"
    r"|\bfully\s+remote\b|\b100%\s*remote\b"
    r"|\bremote\s+(?:work|position|role|job|friendly)\b"
    r"|\bwork\s+remotely\b"
    r"|(?:full[\s-]?)?remote\s+option",
    re.IGNORECASE)

# Рядом с теми же словами часто стоит отказ — «удалённая работа не
# предполагается». Такие совпадения не считаем.
# Гибридная оговорка рядом с совпадением: «частично удалённая работа»,
# «2 дня в офисе» - это гибрид, а не удалёнка по договорённости.
HYBRID_NEARBY = re.compile(
    r"частичн\w*\s+удал|гибрид|\d\s*(?:дн\w+|раза?)\s+в\s+(?:офисе|неделю)"
    r"|\d\s*/\s*\d\s*(?:дн|в\s+офисе)|не\s+мен\w+\s+\d\s*дн",
    re.IGNORECASE)

REMOTE_DENIAL = re.compile(
    r"не\s+предполага|не\s+рассматрива|не\s+предусмотрен|без\s+удал[ёе]нк"
    r"|только\s+офис|исключительно\s+в\s+офисе|нет\s+удал[ёе]нк"
    r"|удал[ёе]нн\w+\s+работа\s+невозможна|не\s+удал[ёе]нн",
    re.IGNORECASE)

REMOTE_WINDOW = 90      # столько символов вокруг совпадения смотрим на отказ
ENRICH_LIMIT = 60       # сколько вакансий за цикл догружаем ради описания


# Мусор из разметки: если в куске текста есть кавычки с двоеточием или
# фигурные скобки, это остаток JSON, а не фраза из описания.
_JSON_JUNK = re.compile(r'["{}\[\]]')


def _clean_quote(raw):
    """Приводит кусок описания к читаемой фразе или возвращает None."""
    quote = re.sub(r"\s+", " ", raw).strip()
    if _JSON_JUNK.search(quote):
        return None
    # Обрезаем оборванные слова по краям, чтобы фраза начиналась
    # и заканчивалась целым словом.
    quote = re.sub(r"^\S*?\s", "", quote, count=1)
    quote = re.sub(r"\s\S*$", "", quote, count=1)
    # Повисший предлог в конце фразы читается как обрыв
    quote = re.sub(r"\s(?:в|во|из|на|и|с|со|по|для|от|к|у|о|об|при|за)$",
                   "", quote, flags=re.IGNORECASE)
    quote = quote.strip(" ,;:-—·|")
    return quote if len(quote) >= 15 else None


def remote_mentioned(text):
    """Фраза, которой в описании обещают удалёнку, или None.

    Возвращаем именно фразу, чтобы потом показать её в карточке: обещание
    «удалёнка по договорённости» должно быть проверяемым, а не на веру.
    """
    plain = _strip_tags(text)
    for m in REMOTE_IN_TEXT.finditer(plain):
        around = plain[max(0, m.start() - REMOTE_WINDOW):m.end() + REMOTE_WINDOW]
        if REMOTE_DENIAL.search(around) or HYBRID_NEARBY.search(around):
            continue
        quote = _clean_quote(plain[max(0, m.start() - 40):m.end() + 60])
        if quote:
            return quote
    return None


# --------------------------------------------------------- IT-аккредитация

# Про аккредитацию компании в Минцифры пишут по-разному: «аккредитованная
# IT-компания», «компания с IT-аккредитацией», «есть аккредитация Минцифры»,
# «accredited IT company». Ловим все формы написания.
IT_ACCRED = re.compile(
    r"(?:ай[\s-]?ти|\bit\b|\bит\b)[\s-]{0,3}аккредит"
    r"|аккредит\w*\s+(?:\w+\s+){0,3}(?:ай[\s-]?ти|\bit\b|\bит\b)"
    r"|аккредит\w*\s+(?:\w+\s+){0,2}минцифр"
    r"|минцифр\w*[^.]{0,40}аккредит"
    r"|аккредитованн\w+\s+(?:компания|компании|организация|работодател)"
    r"|аккредитац\w+\s+(?:компании|организации)"
    r"|государствен\w+\s+аккредитац\w+\s+(?:в\s+сфере\s+)?(?:ит|информационных)"
    r"|accredited\s+it\b|it\s+accreditation",
    re.IGNORECASE)

# Прямое отрицание: отвечаем «нет», а не «нет информации».
ACCRED_DENIAL = re.compile(
    r"не\s+аккредитован|нет\s+(?:ит|it|ай[\s-]?ти)[\s-]?аккредитац"
    r"|аккредитаци\w*\s+нет|без\s+(?:ит|it|ай[\s-]?ти)[\s-]?аккредитац"
    r"|no\s+it\s+accreditation",
    re.IGNORECASE)


def it_accredited(text):
    """«yes» — аккредитация есть, «no» — её прямо отрицают, None — молчат."""
    plain = _strip_tags(text)
    if ACCRED_DENIAL.search(plain):
        return "no"
    return "yes" if IT_ACCRED.search(plain) else None


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


async def accred_by_name(client, company):
    """Аккредитация компании по названию — через выдачу hh.

    Отвечаем только «yes»: у крупных компаний на hh несколько юрлиц
    («Авито: бэк-офис», «Яндекс Крауд»), и отметка у них разная, поэтому
    «нет» по названию — это гадание, а не факт. Результат запоминаем.
    """
    name = (company or "").strip()
    if len(name) < 3:
        return ""
    known = db.accred_cached(name)
    if known is not None:
        return known

    found = ""
    try:
        resp = await _hh_get(client, "https://hh.ru/search/vacancy",
                             {"text": name, "area": 113, "items_on_page": 20},
                             {"User-Agent": UA})
        if resp is not None:
            m = re.search(
                r'<template[^>]*id="HH-Lux-InitialState"[^>]*>(.*?)</template>',
                resp.text, re.S)
            if m:
                vacs = (json.loads(html_mod.unescape(m.group(1)))
                        .get("vacancySearchResult") or {}).get("vacancies") or []
                low = name.lower()
                for raw in vacs:
                    comp = raw.get("company") or {}
                    cname = (comp.get("visibleName") or comp.get("name") or "").lower()
                    if (low in cname or cname in low) and comp.get("accreditedITEmployer"):
                        found = "yes"
                        break
    except Exception as exc:
        logger.info("аккредитация %s: не спросил: %s %s", name, type(exc).__name__, exc)
        return ""

    db.remember_accred(name, found)
    return found


async def enrich_remote(items, limit=ENRICH_LIMIT):
    """Дочитывает описания вакансий, у которых в шапке удалёнки нет.

    Дорого (по запросу на вакансию), поэтому вызывается только для новых
    подходящих вакансий и не больше limit штук за цикл.
    """
    # Раньше смотрели только hh, и из-за этого терялся весь LinkedIn: он
    # формат работы в выдаче вообще не публикует, пустой формат считался
    # офисом, и при фильтре «только удалёнка» вакансии молча отсеивались.
    # Описание читаем у всех непроверенных, а не только у «не удалёнки»:
    # помимо скрытой удалёнки оттуда же берём маркер IT-аккредитации.
    targets = [v for v in items
               if v.get("url") and not v.get("remote_checked")][:limit]
    if not targets:
        return 0

    found = accred = 0
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for vac in targets:
            try:
                if vac.get("source") == "hh":
                    description = await fetch_hh_description(client, vac["ext_id"])
                else:
                    resp = await client.get(vac["url"], headers={"User-Agent": UA})
                    description = resp.text if resp.status_code == 200 else ""
            except Exception as exc:
                logger.info("не смог прочитать описание %s: %s %s",
                            vac["uid"], type(exc).__name__, exc)
                continue
            vac["remote_checked"] = True
            # hh сам отмечает аккредитованных работодателей - его ответ
            # точнее слов в описании, поэтому не затираем.
            if description and not vac.get("it_accred"):
                vac["it_accred"] = it_accredited(description) or ""
            # Вакансия не с hh и в описании ничего нет - спросим у hh про
            # саму компанию: она могла не публиковаться там этой вакансией,
            # но сама в реестре быть.
            if not vac.get("it_accred") and vac.get("source") != "hh":
                vac["it_accred"] = await accred_by_name(client, vac.get("company"))
                if vac["it_accred"] == "yes":
                    accred += 1
            quote = remote_mentioned(description) if description else None
            if quote:
                formats = [f for f in (vac.get("work_format") or "").split(",") if f]
                formats.append("remote_maybe")
                vac["work_format"] = ",".join(dict.fromkeys(formats))
                vac["remote_note"] = quote[:200]
                found += 1
    logger.info("описаний прочитано %s, скрытой удалёнки найдено %s,"
                " с IT-аккредитацией %s", len(targets), found, accred)
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


# -------------------------------------------------------------------- Авито

# career.avito.com рисует карточки скриптом, но данные уже лежат в HTML
# атрибутами data-vacancy-*: id, раздел, города и готовая пометка удалёнки.
# На главной по четыре вакансии из раздела, весь список - на страницах
# разделов, поэтому обходим ещё и профильные для нас.
AVITO_HOST = "https://career.avito.com"
AVITO_PAGES = [
    "/vacancies/",
    "/vacancies/analitika-dannykh",
    "/vacancies/upravlenie-produktom",
    "/vacancies/dizayn",
    "/vacancies/marketing",
    "/vacancies/klientskiy-servis",
    "/vacancies/ux-redaktsiya",
    "/vacancies/data-science",
]
AVITO_HREF = re.compile(r'href="(/vacancies/[\w-]+/(\d+)/?)"')
AVITO_NAME = re.compile(r'item-name"[^>]*>(.*?)</a>', re.S)
AVITO_ATTR = re.compile(r'data-vacancy-(\w+)="([^"]*)"')


def _avito_cards(text):
    for chunk in text.split("data-vacancy-id=")[1:]:
        head = chunk[:1500]
        m_href = AVITO_HREF.search(head)
        m_name = AVITO_NAME.search(head)
        if not m_href or not m_name:
            continue
        attrs = dict(AVITO_ATTR.findall(head))
        remote = (attrs.get("remote") or "").strip().lower().startswith("да")
        yield {
            "uid": "avito:" + m_href.group(2),
            "source": "avito",
            "ext_id": m_href.group(2),
            "title": _strip_tags(m_name.group(1)),
            "company": "Авито",
            "area": (attrs.get("geo") or "").strip()[:60],
            "url": AVITO_HOST + m_href.group(1),
            # даты на сайте нет - считаем вакансию свежей с момента находки
            "published_at": _now_iso(),
            "salary_from": None,
            "salary_to": None,
            "currency": "",
            "work_format": "remote" if remote else "office",
            "experience": "",
        }


async def fetch_avito(client):
    out, seen = [], set()
    for path in AVITO_PAGES:
        try:
            resp = await client.get(AVITO_HOST + path, headers={"User-Agent": UA})
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("avito: %s не открылась: %s %s", path, type(exc).__name__, exc)
            continue
        for item in _avito_cards(resp.text):
            if item["uid"] in seen:
                continue
            seen.add(item["uid"])
            out.append(item)
        await asyncio.sleep(0.5)
    return out


# --------------------------------------------------------------------- 2ГИС

# job.2gis.ru отдаёт список прямо в HTML, без отдельного API. Классы там
# генерируются сборщиком (chipLabel-dBvEm5), поэтому цепляемся за устойчивое:
# ссылку вида /vacancies/<направление>/<id>, заголовок с data-slot="heading"
# и чипы с названием города или пометкой «Удалённо».
GIS_URL = "https://job.2gis.ru/vacancies"
GIS_ID = re.compile(r'href="(/vacancies/([a-z_]+)/(\d+))"')
GIS_TITLE = re.compile(r'<h\d[^>]*data-slot="heading"[^>]*>(.*?)</h\d>', re.S)
GIS_CHIP = re.compile(r'chipLabel-[^"]*">(.*?)</span>', re.S)


async def fetch_2gis(client):
    try:
        resp = await client.get(GIS_URL, headers={"User-Agent": UA})
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("2gis: страница не открылась: %s %s", type(exc).__name__, exc)
        return []

    out = []
    for chunk in resp.text.split('<a href="/vacancies/')[1:]:
        m_id = GIS_ID.search('href="/vacancies/' + chunk[:200])
        m_title = GIS_TITLE.search(chunk)
        if not m_id or not m_title:
            continue
        chips = [_strip_tags(c) for c in GIS_CHIP.findall(chunk[:2000])]
        remote = any("удал" in c.lower() for c in chips)
        # последний чип - это город или «Удалённо», остальные про направление
        city = "" if remote else (chips[-1] if chips else "")
        out.append({
            "uid": "2gis:" + m_id.group(3),
            "source": "2gis",
            "ext_id": m_id.group(3),
            "title": _strip_tags(m_title.group(1)).replace(" ", " "),
            "company": "2ГИС",
            "area": city,
            "url": "https://job.2gis.ru" + m_id.group(1),
            # даты на сайте нет - считаем вакансию свежей с момента находки
            "published_at": _now_iso(),
            "salary_from": None,
            "salary_to": None,
            "currency": "",
            "work_format": "remote" if remote else "office",
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


async def fetch_habr(client, query, remote=False, pages=HABR_PAGES):
    out, seen = [], set()
    for page in range(1, pages + 1):
        got = await _habr_page(client, query, remote, page)
        # Считать «страница неполная - значит последняя» тут нельзя: на
        # странице 25 карточек, но часть из них другого вида, и разбор даёт
        # то 11, то 20. Останавливаемся, когда новых вакансий больше нет.
        fresh = [v for v in got if v["uid"] not in seen]
        if not fresh:
            break
        seen.update(v["uid"] for v in fresh)
        out.extend(fresh)
    return out


async def _habr_page(client, query, remote, page):
    params = {"q": query, "type": "all", "sort": "date", "page": page}
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
        logger.warning("habr: запрос не удался (%s, стр. %s): %s", query, page, exc)
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
# LinkedIn жёстко режет запросы с серверных адресов: на хостинге почти каждый
# запрос отвечал 429. Поэтому ходим по одному, с паузой, а поймав отказ -
# отступаем на пару часов вместо того, чтобы долбить его каждые полчаса.
LI_LIMIT = asyncio.Semaphore(1)
LI_PAUSE = 3.0
LI_COOLDOWN_HOURS = 2
_li_blocked_until = None
LI_ID = re.compile(r'data-entity-urn="urn:li:jobPosting:(\d+)"')
LI_LINK = re.compile(r'base-card__full-link"[^>]*href="([^"?]+)')
LI_TITLE = re.compile(r'base-search-card__title"[^>]*>(.*?)</h3>', re.S)
LI_COMPANY = re.compile(r'base-search-card__subtitle"[^>]*>.*?<a[^>]*>(.*?)</a>', re.S)
LI_PLACE = re.compile(r'job-search-card__location"[^>]*>(.*?)</span>', re.S)
LI_DATE = re.compile(r'<time[^>]*datetime="([^"]+)"')


# Где ищем: Россия и весь мир. Список можно переопределить переменной
# LINKEDIN_LOCATIONS через запятую.
LINKEDIN_LOCATIONS = [loc.strip() for loc in os.getenv(
    "LINKEDIN_LOCATIONS", "Russian Federation,Worldwide").split(",") if loc.strip()]
LI_PAGES = 3            # выдача отдаёт по 10 вакансий на страницу
_li_turn = 0


# Признак российского рынка живёт в matching: им пользуется и сбор с
# LinkedIn, и фильтр пользователя.
looks_russian = matching.looks_russian


def _linkedin_next(queries):
    """Следующая пара «запрос + локация» по кругу - одна на цикл сбора."""
    global _li_turn
    pairs = [(q, loc) for q in (queries or ["researcher"])
             for loc in LINKEDIN_LOCATIONS]
    query, location = pairs[_li_turn % len(pairs)]
    _li_turn += 1
    return query, location


async def fetch_linkedin(client, query, location="Russian Federation",
                         period_days=30, pages=LI_PAGES):
    """Гостевая выдача LinkedIn. Формат работы там не публикуется, а фильтр
    f_WT=2 («Remote») выдачу не меняет - проверено. Поэтому удалёнку
    определяем по тексту названия и локации («… (Remote)»)."""
    out = []
    for page in range(pages):
        chunk_out = await _fetch_linkedin_page(client, query, location,
                                               period_days, page * 10)
        out.extend(chunk_out)
        if len(chunk_out) < 10:
            break
    return out


async def _fetch_linkedin_page(client, query, location, period_days, start):
    global _li_blocked_until
    if _li_blocked_until and datetime.now(MSK) < _li_blocked_until:
        return []

    params = {"keywords": query, "location": location, "start": start,
              "f_TPR": "r" + str(period_days * 86400)}
    try:
        async with LI_LIMIT:
            resp = await client.get(LI_URL, params=params, headers={"User-Agent": UA})
            await asyncio.sleep(LI_PAUSE)
        if resp.status_code == 429:
            _li_blocked_until = datetime.now(MSK) + timedelta(hours=LI_COOLDOWN_HOURS)
            logger.info("linkedin: отказ по частоте, отступаю до %s",
                        _li_blocked_until.strftime("%H:%M"))
            return []
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("linkedin: запрос не удался (%s / %s): %s %s",
                       query, location, type(exc).__name__, exc)
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
    # Иностранные компании без российского признака отбрасываем: релокация в
    # другую страну человеку не нужна.
    russian = [v for v in out if looks_russian(v)]
    if len(russian) != len(out):
        logger.info("linkedin: отброшено не про российский рынок %s из %s",
                    len(out) - len(russian), len(out))
    return russian


# ----------------------------------------------------------------- getmatch

async def fetch_getmatch(client, limit=100, pages=GETMATCH_PAGES):
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
    # keyword ищет по всему тексту вакансии - нам нужен только заголовок,
    # поэтому берём расширенный поиск: srws=1 это «должность», skwc=or
    # «хотя бы одно слово». Период задаём датой: параметр period понимает
    # только 1, 3, 7 и 0, а нам нужно окно в 30 дней.
    since = int(datetime.now(MSK).timestamp()) - period_days * 86400
    params = [
        ("keywords[0][srws]", 1),
        ("keywords[0][skwc]", "or"),
        ("keywords[0][keys]", query),
        ("date_published_from", since),
        ("order_field", "date"),
        ("count", count),
    ]
    try:
        resp = await client.get(
            "https://api.superjob.ru/2.0/vacancies/",
            params=params,
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
            # place_of_work: 2 - «на дому», то есть удалёнка
            "work_format": "remote" if (raw.get("place_of_work") or {}).get("id") == 2 else "",
            "experience": (raw.get("experience") or {}).get("title") or "",
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


# Собственные карьерные сайты компаний - у них приоритет над агрегаторами.
OWN_SITE_SOURCES = {"careers", "magnit", "avito", "2gis", "dodo", "aviasales", "itone"}


def merge_duplicates(items):
    """Склеить одну и ту же вакансию, найденную в разных сервисах.

    Раньше дубли убирались только по uid, а он начинается с имени источника
    («hh:123», «habr:456»), поэтому одна вакансия на hh и на Хабре приходила
    дважды. Теперь такие карточки объединяются, а ссылки на все сервисы,
    где вакансия нашлась, собираются в links.
    """
    # Сначала карьерные сайты компаний: если вакансия есть и у них, и на
    # агрегаторе, основной становится карточка с сайта работодателя, а
    # ссылка на агрегатор просто добавляется второй кнопкой.
    items = sorted(items, key=lambda v: 0 if v.get("source") in OWN_SITE_SOURCES else 1)

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


# ------------------------------------------------------------- Магнит Tech

# Свой сайт вакансий на Vue, но данные лежат в открытом JSON.
MAGNIT_API = "https://magnit.tech/api/v1/vacancy"


async def fetch_magnit(client):
    out = []
    for page in range(1, 6):
        try:
            resp = await client.get(MAGNIT_API, params={"page": page, "per_page": 50},
                                    headers={"User-Agent": UA,
                                             "Accept": "application/json"})
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("magnit: запрос не удался: %s %s", type(exc).__name__, exc)
            break

        items = data.get("results") or []
        for raw in items:
            vid = str(raw.get("id") or raw.get("foreign_id") or "")
            title = (raw.get("title") or "").strip()
            if not vid or not title:
                continue
            # Формат работы у них описан фразой: «Москва и Краснодар
            # (гибрид/офис), другие города - удаленно».
            where = " ".join(f.get("name") or ""
                             for f in (raw.get("work_formats") or []))
            formats = []
            if re.search(r"удал[ёе]н|remote", where, re.I):
                formats.append("remote")
            if re.search(r"гибрид|hybrid", where, re.I):
                formats.append("hybrid")
            if re.search(r"офис", where, re.I):
                formats.append("office")
            out.append({
                "uid": "magnit:" + vid,
                "source": "magnit",
                "ext_id": vid,
                "title": title,
                "company": "Магнит Tech",
                "area": (raw.get("location") or "").strip(),
                "url": "https://magnit.tech/vacancy/" + vid,
                "published_at": None,
                "salary_from": None,
                "salary_to": None,
                "currency": "RUR",
                "work_format": ",".join(dict.fromkeys(formats)),
                "experience": "",
                # Магнит Tech - аккредитованная ИТ-компания, но утверждать
                # это за них не будем: отметку подтянет общая проверка.
                "it_accred": "",
            })
        if not (data.get("meta") or {}).get("has_more_pages"):
            break
    logger.info("magnit: вакансий %s", len(out))
    return out


# ------------------------------------------- карьерные сайты «на раскрытие»

# Есть сайты, где вакансии - это раскрывающиеся блоки без отдельных адресов
# (Burger King, Orion soft). Ссылки там ведут на «Откликнуться», а название
# лежит просто текстом. Поэтому вытаскиваем подписи и оставляем только те,
# что проходят фильтр профиля; ссылка - на сам раздел вакансий.
TEXT_CAREERS = [
    ("Burger King", "https://bktch.ru/vacancies"),
    ("Orion soft", "https://career.orionsoft.ru/vacancy"),
    ("Монетка", "https://career.monetka.ru/it/"),
    ("Ясно", "https://work.yasno.live/"),
    # Utair IT: имя домена записано в punycode, httpx с кириллицей в адресе
    # работает хуже
    ("Utair IT", "https://xn----ptbb2aie6eg.xn--p1ai/vacancies"),
]
TEXT_NODE = re.compile(r">([^<>]{4,120})<")


def _visible_lines(text):
    body = re.sub(r"<(script|style)\b[^>]*>[\s\S]*?</\1>", " ", text or "",
                  flags=re.IGNORECASE)
    out, seen = [], set()
    for raw in TEXT_NODE.findall(body):
        line = re.sub(r"\s+", " ", html_mod.unescape(raw)).strip()
        if line and line.lower() not in seen:
            seen.add(line.lower())
            out.append(line)
    return out


_SECTION_NAMES = {"аналитика", "разработка", "маркетинг", "дизайн",
                  "тестирование", "поддержка", "данные", "продукт",
                  "вакансии", "карьера", "стажировки", "обучение"}


def _looks_like_title(line):
    """Похоже ли на название вакансии, а не на строку описания.

    На странице без карточек рядом с названиями лежат пункты обязанностей:
    «разработка и сопровождение отчётности» фильтр пропускал как вакансию.
    Название короткое, начинается с заглавной буквы и не содержит ни точки
    с запятой, ни запятой с союзом.
    """
    if not 5 < len(line) <= 70:
        return False
    if ";" in line or line.endswith((".", ":")):
        return False
    head = line.lstrip("#•- ")[:1]
    if not (head.isupper() or head.isdigit()):
        return False
    # «Аналитика» одним словом - это название раздела, а не вакансии;
    # «Аналитик» одним словом - настоящая вакансия, такие есть у Монетки.
    return line.strip().lower() not in _SECTION_NAMES


# Сколько похожих на вакансию строк нашлось на каждой текстовой странице
# за последний обход. Нужно отчёту /sites: карточек с отдельными
# адресами тут нет, и без этого числа «карточек» равнялось бы «по теме»,
# то есть отчёт не показывал бы, читается ли страница целиком.
TEXT_CAREER_SEEN = {}


async def fetch_text_careers(client):
    out = []
    for company, url in TEXT_CAREERS:
        try:
            # Монетка и подобные тяжёлые страницы отвечают медленно
            resp = await client.get(url, headers={"User-Agent": UA}, timeout=25)
            resp.raise_for_status()
        except Exception as exc:
            logger.info("careers %s: страница не открылась: %s %s",
                        company, type(exc).__name__, exc)
            continue

        found = seen = 0
        for line in _visible_lines(resp.text):
            if not _looks_like_title(line):
                continue
            seen += 1
            # Отдельных адресов у вакансий нет, поэтому мусор отсечь больше
            # нечем - сразу спрашиваем фильтр профиля.
            if not matching.classify(line):
                continue
            found += 1
            vid = hashlib.md5((company + line).encode("utf-8")).hexdigest()[:12]
            out.append({
                "uid": "careers:" + vid,
                "source": "careers",
                "ext_id": vid,
                "title": line,
                "company": company,
                "area": "",
                "url": url,
                "published_at": None,
                "salary_from": None,
                "salary_to": None,
                "currency": "RUR",
                "work_format": "",
                "experience": "",
            })
        TEXT_CAREER_SEEN[company] = seen
        logger.info("careers %s: заголовков %s, по теме %s",
                    company, seen, found)
    return out


# ------------------------------------------------------- AliExpress Tech

# it.aliexpress.ru - лендинг на Next.js: в разметке только текст о команде,
# список вакансий страница просит у своего же HR-API. Адрес и имена
# параметров взяты из её собственного скрипта (v1/vacancy/query), пробовать
# другие хосты и угадывать пути я не стал.
ALI_API = "https://aero.aliexpress-inc.ru/v1/vacancy/query"
ALI_PAGE = "https://it.aliexpress.ru/"
ALI_VACANCY = "https://it.aliexpress.ru/vacancy/"


def _json_titles(node, found=None):
    """Собирает из ответа API объекты, похожие на вакансии.

    Структуру ответа заранее не знаем (хост доступен только из России),
    поэтому обходим дерево и берём словари, у которых есть название.
    """
    found = [] if found is None else found
    if isinstance(node, dict):
        title = node.get("title") or node.get("name") or node.get("vacancyName")
        if isinstance(title, str) and 4 < len(title) < 160:
            found.append(node)
        for value in node.values():
            _json_titles(value, found)
    elif isinstance(node, list):
        for value in node:
            _json_titles(value, found)
    return found


async def fetch_aliexpress(client):
    try:
        resp = await client.post(
            ALI_API, timeout=20,
            json={"pageSize": 100, "pageNumber": 1},
            headers={"User-Agent": UA, "Accept": "application/json",
                     "Referer": ALI_PAGE, "Origin": ALI_PAGE.rstrip("/")})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.info("aliexpress: не ответил: %s %s", type(exc).__name__, exc)
        return []

    out = []
    for raw in _json_titles(data):
        title = (raw.get("title") or raw.get("name")
                 or raw.get("vacancyName") or "").strip()
        vid = str(raw.get("id") or raw.get("vacancyId") or "")
        if not title:
            continue
        url = ALI_VACANCY + vid if vid else ALI_PAGE
        out.append({
            "uid": "ali:" + (vid or hashlib.md5(title.encode()).hexdigest()[:12]),
            "source": "careers",
            "ext_id": vid or hashlib.md5(title.encode()).hexdigest()[:12],
            "title": title,
            "company": "AliExpress Tech",
            "area": str(raw.get("city") or raw.get("location") or ""),
            "url": url,
            "published_at": None,
            "salary_from": None,
            "salary_to": None,
            "currency": "RUR",
            "work_format": str(raw.get("workType") or raw.get("format") or ""),
            "experience": str(raw.get("workExperience") or ""),
        })
    logger.info("aliexpress: вакансий %s", len(out))
    return out


# --------------------------------------------------------------- МТС Линк

# Сайт вакансий на Tilda, но список подтягивается из их же открытого JSON
# (он отдаётся с основного домена, на job.* тот же адрес возвращает 404).
MTSLINK_API = "https://mts-link.ru/api/huntflow/vacancies"
MTSLINK_PAGE = "https://job.mts-link.ru/vacancies/"


async def fetch_mtslink(client):
    try:
        resp = await client.get(
            MTSLINK_API, timeout=15,
            headers={"User-Agent": UA, "Accept": "application/json",
                     "Referer": MTSLINK_PAGE})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.info("mts-link: не ответил: %s %s", type(exc).__name__, exc)
        return []

    rows = data if isinstance(data, list) else (data.get("items") or data.get("data") or [])
    out = []
    for raw in rows:
        vid = str(raw.get("id") or "")
        title = (raw.get("position") or "").strip()
        if not vid or not title or raw.get("hidden"):
            continue
        if (raw.get("state") or "OPEN").upper() != "OPEN":
            continue
        where = str(raw.get("workFormat") or "")
        formats = []
        if re.search(r"удал[ёе]нн|remote", where, re.I):
            formats.append("remote")
        if re.search(r"гибрид", where, re.I):
            formats.append("hybrid")
        if re.search(r"офис", where, re.I):
            formats.append("office")
        out.append({
            "uid": "mtslink:" + vid,
            "source": "careers",
            "ext_id": vid,
            "title": title,
            "company": "МТС Линк",
            "area": (raw.get("accountDivision") or "").strip(),
            "url": MTSLINK_PAGE,
            "published_at": None,
            "salary_from": None,
            "salary_to": None,
            "currency": "RUR",
            "work_format": ",".join(dict.fromkeys(formats)),
            "experience": str(raw.get("workExperience") or "").strip(),
        })
    logger.info("mts-link: вакансий %s", len(out))
    return out


# ----------------------------------------------------------------- Skyeng

# Ссылки на вакансии у них - короткие адреса в корне сайта
# (/analitik-dannih), под общий разбор карьерных страниц не подходят.
SKYENG_URL = "https://vacancies.skyeng.ru/"
SKYENG_LINK = re.compile(r'<a[^>]+href="(/[a-z0-9-]{5,90})"[^>]*>(.*?)</a>', re.S)
# В подписи карточки после названия идёт отдел: «Менеджер по продажам
# Продажи». Отрезаем его, иначе отдел попадает в название и ломает фильтр.
SKYENG_DEPT = re.compile(
    r"\s+(?:Продажи|Маркетинг|Клиентский сервис|Разработка|Продукт|Аналитика|"
    r"Финансы|Преподавание|Дизайн|HR|Операции|Юристы).*$")


async def fetch_skyeng(client):
    try:
        resp = await client.get(SKYENG_URL, headers={"User-Agent": UA}, timeout=15)
        resp.raise_for_status()
    except Exception as exc:
        logger.info("skyeng: страница не открылась: %s %s", type(exc).__name__, exc)
        return []

    out, seen = [], set()
    for href, inner in SKYENG_LINK.findall(resp.text):
        title = SKYENG_DEPT.sub("", _strip_tags(inner)).strip()
        if not (4 < len(title) < 90) or href in seen:
            continue
        seen.add(href)
        out.append({
            "uid": "skyeng:" + href.strip("/"),
            "source": "careers",
            "ext_id": href.strip("/"),
            "title": title,
            "company": "Skyeng",
            "area": "",
            "url": SKYENG_URL.rstrip("/") + href,
            "published_at": None,
            "salary_from": None,
            "salary_to": None,
            "currency": "RUR",
            "work_format": "",
            "experience": "",
        })
    logger.info("skyeng: карточек %s", len(out))
    return out


# ------------------------------------------------------- карьерные сайты IT

# Разделы «вакансии» на сайтах IT-компаний. Разметка у всех своя, поэтому
# разбираем двумя общими способами: сначала JSON-LD (schema.org JobPosting),
# который отдаёт половина современных сайтов, потом — ссылки на карточки
# вакансий. Название вакансии всё равно проходит общий фильтр, так что
# пункты меню вроде «Стажировки» отсеются сами.
CAREER_PAGES = [
    # Оставлены только те, что реально открываются с сервера и отдают
    # карточки. Ozon и Самокат отвечают 403 на запросы с хостинга, у Т-Банка
    # хостинг не проверяет сертификат, Lamoda отдаёт пустой каркас на JS.
    # Их вакансии приходят со страниц работодателей на hh. У Skyeng и МТС
    # Линка свои сборщики ниже - там данные лежат в открытом JSON.
    ("Selectel", "https://selectel.ru/careers/all/"),
    ("Яндекс", "https://yandex.ru/jobs/vacancies"),
    ("Контур", "https://kontur.ru/career/vacancies"),
    ("VK", "https://team.vk.company/vacancy/"),
    # Huntflow - конструктор карьерных страниц, разметка у всех одинаковая
    ("Flowwow", "https://flowwow.huntflow.io/"),
    ("Циан", "https://cian.huntflow.io/"),
    ("YCLIENTS", "https://yclients.huntflow.io/"),
    ("Tripster", "https://tripster.huntflow.io/"),
    # Hoff отдаёт 401 всем без куки, Золотое Яблоко - 503 всем, кто не
    # браузер. Их вакансии берём со страниц работодателей на hh
    # (158720 и 776314), а четыре бесполезных запроса за цикл убраны.
    ("Лемана ПРО", "https://rabota.lemanapro.ru/vacancies"),
]

# Сайты, у которых в списке видно только первую страницу найма, зато
# работает поиск: без него аналитики и исследователи там не находились.
CAREER_SEARCH = [
    ("VK", "https://team.vk.company/vacancy/?search={q}"),
    ("Яндекс", "https://yandex.ru/jobs/vacancies?text={q}"),
    # Корень СДЭКа - лендинг без карточек, список отдаётся только поиском.
    ("СДЭК", "https://rabota.cdek.ru/vacancies?query={q}"),
]

# Сколько вакансий страница заявляет у себя сама: «37 вакансий»,
# «Найдено 37», total в её JSON. Нужно, чтобы поймать молчаливую потерю:
# если сайт пишет про 60 вакансий, а карточек разобралось 8, значит список
# докручивается скриптом и мы видим только первый экран.
CLAIM_PATTERNS = [
    re.compile(r"(\d{1,4})\s*ваканс", re.I),
    re.compile(r"найдено[^\d]{0,12}(\d{1,4})", re.I),
    re.compile(r'"total(?:Count|Items|Vacancies)?"\s*:\s*(\d{1,4})', re.I),
]
# Сколько карточек и сколько заявлено - по компаниям за последний обход.
CAREER_CLAIMED = {}


def _claimed_total(text):
    """Наибольшее правдоподобное число вакансий, названное самой страницей."""
    found = set()
    for pattern in CLAIM_PATTERNS:
        for raw in pattern.findall(text):
            number = int(raw)
            # Однозначные числа и маркетинговые «более 5000» только мешают
            if 2 < number < 2000:
                found.add(number)
    return max(found) if found else 0


LD_BLOCK = re.compile(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)
# Ссылка именно на карточку вакансии, а не на раздел: после vacancy/job
# должен быть ещё один кусок пути - идентификатор.
CAREER_LINK = re.compile(
    r'<a[^>]+href="([^"#]*/(?:vacanc(?:y|ies)|vakansii|job|jobs|career)s?/[^"#?]+)"[^>]*>(.*?)</a>',
    re.S | re.I)


def _ld_jobs(text):
    """Вакансии из разметки schema.org, если сайт её отдаёт."""
    out = []
    for block in LD_BLOCK.findall(text):
        try:
            data = json.loads(block.strip())
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            items = data["@graph"]
        for raw in items:
            if not isinstance(raw, dict) or raw.get("@type") != "JobPosting":
                continue
            loc = raw.get("jobLocation") or {}
            if isinstance(loc, list):
                loc = loc[0] if loc else {}
            addr = (loc or {}).get("address") or {}
            out.append({
                "title": _strip_tags(str(raw.get("title") or "")),
                "url": raw.get("url") or "",
                "area": str(addr.get("addressLocality") or "") if isinstance(addr, dict) else "",
                "published_at": (raw.get("datePosted") or None),
            })
    return out


def _page_links(text, base):
    """Запасной разбор: ссылки, похожие на карточки вакансий."""
    from urllib.parse import urljoin

    out, seen = [], set()
    for match in CAREER_LINK.finditer(text):
        href, inner = match.group(1), match.group(2)
        # Внутри карточки название обычно в заголовке, а рядом - зарплата,
        # город, опыт и график. Берём заголовок, если он есть.
        head = CARD_HEADING.search(inner)
        title = _clean_card_title(head.group(1) if head else inner)
        if not title:
            # У Selectel и подобных ссылка - это кнопка «Подробнее», а
            # название вакансии лежит перед ней, вне ссылки. Раньше такие
            # карточки отбрасывались целиком: 4 из 28 вместо всех.
            title = _title_before(text, match.start())
        if not title:
            continue
        url = urljoin(base, href)
        if url in seen:
            continue
        seen.add(url)
        out.append({"title": title, "url": url, "area": "", "published_at": None})
    return out


def _clean_card_title(raw):
    """Чистит название из куска разметки; пусто - значит не название."""
    title = CARD_TAIL.sub("", _strip_tags(raw)).strip(" ,·|-")
    if not (5 < len(title) < 90) or title.lower() in _CAREER_NOISE:
        return ""
    return title


def _title_before(text, pos):
    """Ищет название вакансии в разметке перед ссылкой-кнопкой.

    Берём ближайший к ссылке заголовок или элемент с «title» в классе -
    в карточке он стоит сразу над кнопкой.
    """
    window = text[max(0, pos - 1500):pos]
    found = []
    for pattern in (CARD_HEADING, CARD_TITLE_CLASS):
        for match in pattern.finditer(window):
            title = _clean_card_title(match.group(1))
            if title:
                found.append((match.start(), title))
    # Ближайшее к ссылке, то есть последнее по разметке
    return max(found)[1] if found else ""


CARD_HEADING = re.compile(r"<h[1-6][^>]*>(.*?)</h[1-6]>", re.S | re.IGNORECASE)
# Название карточки без заголовка: <div class="...__title ...">Название</div>
CARD_TITLE_CLASS = re.compile(
    r'<(?:div|span|p)[^>]+class="[^"]*(?:title|name|heading)[^"]*"[^>]*>(.*?)</(?:div|span|p)>',
    re.S | re.IGNORECASE)
# Хвост карточки: зарплата, город, опыт, график - всё, что идёт после
# названия одной строкой.
CARD_TAIL = re.compile(
    r"\s\d[\d\s\u00a0\u202f]*(?:₽|руб|rub).*$"
    r"|\s(?:от|до)\s\d[\d\s\u00a0\u202f]*.*$"
    r"|\s*з/п\s.*$"
    r"|\s*зарплата\s.*$",
    re.IGNORECASE | re.S)


_CAREER_NOISE = {"вакансии", "все вакансии", "стажировки", "карьера", "подробнее",
                 "вакансия", "смотреть вакансии", "открытые вакансии", "jobs",
                 "vacancies", "все вакансии →", "хочу в команду"}


async def fetch_careers(client):
    """Обходит карьерные разделы IT-компаний."""
    out = []
    pages = list(CAREER_PAGES)
    for company, template in CAREER_SEARCH:
        for query in matching.CAREER_QUERIES:
            pages.append((company, template.format(q=quote(query))))
    for company, url in pages:
        # Часть сайтов отдаёт 403/404 на прямой адрес раздела, но открывает
        # корень; ещё часть просит обычные браузерные заголовки.
        headers = {"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9",
                   "Accept": "text/html,application/xhtml+xml"}
        resp = None
        for attempt in (url, re.sub(r"(https?://[^/]+/).*", r"\1", url)):
            try:
                got = await client.get(attempt, headers=headers, timeout=12)
                got.raise_for_status()
                resp = got
                break
            except Exception as exc:
                logger.info("careers %s: %s не открылся: %s %s",
                            company, attempt, type(exc).__name__, exc)
        if resp is None:
            continue
        found = _ld_jobs(resp.text) or _page_links(resp.text, str(resp.url))
        for raw in found:
            if not raw["title"] or not raw["url"]:
                continue
            vid = hashlib.md5(raw["url"].encode("utf-8")).hexdigest()[:12]
            out.append({
                "uid": "careers:" + vid,
                "source": "careers",
                "ext_id": vid,
                "title": raw["title"],
                "company": company,
                "area": raw["area"],
                "url": raw["url"],
                "published_at": raw["published_at"],
                "salary_from": None,
                "salary_to": None,
                "currency": "RUR",
                "work_format": "",
                "experience": "",
            })
        claimed = _claimed_total(resp.text)
        known = CAREER_CLAIMED.get(company, (0, 0))
        # У VK, Яндекса и СДЭКа несколько страниц за обход (поиск по
        # запросам): берём лучший результат, а не последний.
        CAREER_CLAIMED[company] = (max(known[0], len(found)),
                                   max(known[1], claimed))
        logger.info("careers %s: карточек %s, страница заявляет %s",
                    company, len(found), claimed)
    return out


async def fetch_all(sources, hh_queries, habr_queries, area=113, period=7,
                    linkedin_queries=None):
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
        if "rabota" in sources:
            tasks += [fetch_rabota(client, q) for q in habr_queries]
        if "trudvsem" in sources:
            tasks += [fetch_trudvsem(client, q) for q in habr_queries]
        if "zarplata" in sources:
            for q in hh_queries:
                tasks.append(fetch_zarplata(client, q, area=area, period=period,
                                            pages=HH_PAGES))
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
            # С адресов хостинга LinkedIn отвечает 429 почти на каждый
            # запрос, а мы просили у него 24 страницы за цикл. Теперь за
            # цикл берём одну пару «запрос + локация», по кругу: полный
            # круг проходит за несколько часов, зато без отказов.
            tasks.append(fetch_linkedin(client,
                                        *_linkedin_next(linkedin_queries or habr_queries),
                                        period_days=period))
        if "companies" in sources:
            tasks += [fetch_hh_employer(client, eid, period=period)
                      for eid in HH_EMPLOYERS]
            tasks.append(fetch_aviasales(client))
            tasks.append(fetch_dodo(client))
            tasks.append(fetch_2gis(client))
            tasks.append(fetch_avito(client))
            tasks.append(fetch_careers(client))
            tasks.append(fetch_magnit(client))
            tasks.append(fetch_skyeng(client))
            tasks.append(fetch_text_careers(client))
            tasks.append(fetch_mtslink(client))
            tasks.append(fetch_aliexpress(client))
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
