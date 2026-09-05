"""
Telegram-бот: радар вакансий для исследователей.

Каждые CHECK_INTERVAL_MINUTES минут обходит hh.ru, Хабр Карьеру и getmatch,
отбирает вакансии по исследовательским ключам (UX / CX / Product / Marketing
Research, социолог, исследователь пользовательского опыта и т.д.) и присылает
новое в личку. Дубли не приходят: каждая вакансия отправляется один раз.

Команды:
  /start    - подписаться и получить свежую подборку
  /find     - проверить источники прямо сейчас
  /search   - разовый поиск по своему запросу (hh.ru)
  /settings - регион, удалёнка, зарплата, категории, режим уведомлений
  /keywords - личные ключевые и стоп-слова
  /pause    - поставить рассылку на паузу, /resume - снять
  /stats    - статистика
  /help     - справка
"""

import asyncio
import logging
import os
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, NetworkError, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import db
import matching
import search

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "30"))
SEARCH_PERIOD_DAYS = int(os.getenv("SEARCH_PERIOD_DAYS", "30"))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

MAIN_MENU = ReplyKeyboardMarkup(
    [
        ["🔍 Найти сейчас", "📊 Статистика"],
        ["⚙️ Настройки", "🔑 Ключевые слова"],
        ["⏸ Пауза", "❓ Помощь"],
    ],
    resize_keyboard=True,
)

REGIONS = {"any": "Вся Россия", "msk": "Москва", "spb": "Санкт-Петербург"}
SALARY_STEPS = [0, 100000, 150000, 200000, 250000, 300000]
SOURCE_NAMES = {"hh": "hh.ru", "habr": "Хабр Карьера", "getmatch": "getmatch",
                "tg": "Telegram", "geekjob": "Geekjob",
                "itone": "IT_One", "superjob": "SuperJob", "linkedin": "LinkedIn"}
CURRENCY = {"RUR": "₽", "RUB": "₽", "USD": "$", "EUR": "€", "KZT": "₸", "BYR": "Br"}
WORK_FORMAT = {"remote": "удалённо", "hybrid": "гибрид", "office": "офис"}
WORK_FORMAT_ORDER = ["remote", "hybrid", "office"]


def format_names(value):
    formats = [f for f in (value or "").split(",") if f]
    formats.sort(key=lambda f: WORK_FORMAT_ORDER.index(f)
                 if f in WORK_FORMAT_ORDER else 9)
    return " / ".join(WORK_FORMAT.get(f, f) for f in formats)


# ------------------------------------------------------------------- вывод

def money(value, currency):
    return "{:,}".format(int(value)).replace(",", " ") + " " + CURRENCY.get(currency, currency or "")


def salary_line(vac):
    s_from, s_to = vac.get("salary_from"), vac.get("salary_to")
    cur = vac.get("currency") or "RUR"
    if s_from and s_to:
        if s_from == s_to:
            return money(s_from, cur)
        return money(s_from, cur) + " – " + money(s_to, cur)
    if s_from:
        return "от " + money(s_from, cur)
    if s_to:
        return "до " + money(s_to, cur)
    return ""


def escape(text):
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_vacancy(vac):
    cats = [c for c in (vac.get("categories") or "").split(",") if c]
    icons = "".join(matching.category_label(c)[0] for c in cats if c in matching.ALL_CATEGORY_IDS)
    head = (icons + " " if icons else "") + "<b>" + escape(vac["title"]) + "</b>"

    second = []
    if vac.get("company"):
        second.append("🏢 " + escape(vac["company"]))
    place = vac.get("area") or ""
    fmt = format_names(vac.get("work_format"))
    if place:
        second.append("📍 " + escape(place))
    if fmt:
        second.append("🏠 " + fmt)

    lines = [head]
    if second:
        lines.append(" · ".join(second))
    # Зарплату пишем всегда: её отсутствие — тоже информация о вакансии.
    lines.append("💰 " + (salary_line(vac) or "не указана"))

    tail = []
    if vac.get("experience"):
        tail.append("опыт: " + vac["experience"])
    tail.append(vac.get("published_at", "")[:10])
    lines.append("🧭 " + " · ".join(t for t in tail if t))

    # Одна вакансия может висеть сразу на нескольких сервисах — тогда
    # у карточки будет несколько ссылок, по одной на каждый.
    links = vac.get("links") or [(vac.get("source", ""), vac.get("url", ""))]
    seen_urls, buttons = set(), []
    for source, url in links:
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        label = SOURCE_NAMES.get(source, source) or "Открыть"
        buttons.append(InlineKeyboardButton(label, url=url))

    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    rows.append([
        InlineKeyboardButton("🚫 Скрыть компанию", callback_data="mute:" + vac["uid"])
    ])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# ------------------------------------------------------------------ сбор

async def collect(sources=None):
    """Опрашивает источники, размечает категории, кладёт в базу."""
    sources = sources or set(db.DEFAULT_SOURCES.split(","))
    items = await search.fetch_all(
        sources, matching.HH_QUERIES, matching.HABR_QUERIES,
        area=113, period=SEARCH_PERIOD_DAYS,
    )
    for vac in items:
        cats = matching.classify(vac["title"])
        vac["relevant"] = cats is not None
        vac["categories"] = cats or []
    fresh = db.upsert_vacancies(items)
    logger.info("собрано %s вакансий, новых %s", len(items), len(fresh))
    return items, fresh


def user_filters(user_id):
    return (
        db.get_keywords(user_id, "include"),
        db.get_keywords(user_id, "exclude"),
        set(db.get_muted(user_id)),
    )


def pick_for_user(st, limit=None):
    includes, excludes, muted = user_filters(st["user_id"])
    # окно ленты совпадает с окном сбора, иначе часть собранного
    # никогда не доедет до человека
    pending = db.unsent_for_user(st["user_id"], days=SEARCH_PERIOD_DAYS)
    picked = [v for v in pending if db.matches_user(v, st, includes, excludes, muted)]
    picked.reverse()  # сначала то, что постарше - лента читается сверху вниз
    if limit:
        picked = picked[-limit:]
    return picked


def may_send_now(st):
    hour = db.now().hour
    start, end = int(st.get("quiet_start", 23)), int(st.get("quiet_end", 8))
    quiet = (start <= hour or hour < end) if start > end else (start <= hour < end)
    if quiet:
        return False
    if st.get("mode") == "digest":
        return hour == int(st.get("digest_hour", 10))
    return True


async def send_vacancies(bot, chat_id, vacancies):
    sent_uids = []
    for vac in vacancies:
        text, keyboard = format_vacancy(vac)
        try:
            await bot.send_message(
                chat_id=chat_id, text=text, reply_markup=keyboard,
                parse_mode=ParseMode.HTML, disable_web_page_preview=True,
            )
            sent_uids.append(vac["uid"])
            await asyncio.sleep(0.4)
        except Forbidden:
            raise
        except TelegramError as exc:
            logger.warning("не отправилось (%s): %s", vac["uid"], exc)
    return sent_uids


async def deliver_all(app):
    for user in db.active_users():
        st = db.get_settings(user["user_id"])
        if not may_send_now(st):
            continue
        picks = pick_for_user(st, limit=int(st.get("max_per_run", 8)))
        if not picks:
            continue
        try:
            sent_uids = await send_vacancies(app.bot, user["chat_id"] or user["user_id"], picks)
        except Forbidden:
            logger.info("пользователь %s заблокировал бота - ставлю на паузу", user["user_id"])
            db.set_setting(user["user_id"], "paused", 1)
            continue
        db.mark_sent(user["user_id"], sent_uids)
        logger.info("пользователю %s отправлено %s вакансий", user["user_id"], len(sent_uids))


async def poll_loop(app):
    await asyncio.sleep(10)
    while True:
        try:
            sources = set()
            for user in db.active_users():
                sources |= set((user.get("sources") or db.DEFAULT_SOURCES).split(","))
            if sources:
                await collect(sources)
                await deliver_all(app)
            db.cleanup()
        except Exception:
            logger.exception("цикл опроса упал, продолжаю через интервал")
        await asyncio.sleep(CHECK_INTERVAL_MINUTES * 60)


# -------------------------------------------------------------- обработчики

WELCOME = (
    "🔎 <b>Радар вакансий для исследователей</b>\n\n"
    "Слежу за hh.ru, Хабр Карьерой и getmatch и присылаю новые вакансии:\n"
    "UX / CX / Product / Market Research, исследователь пользовательского и "
    "клиентского опыта, социолог, продуктовый исследователь, CustDev.\n\n"
    "Ищу строго по названию вакансии, аналитику данных и продажи отсекаю.\n"
    "По умолчанию присылаю <b>только удалёнку</b> — офис и гибрид включаются "
    "в настройках.\n\n"
    "Проверяю раз в {interval} мин. Каждая вакансия приходит один раз.\n\n"
    "⚙️ Настройки — формат работы, регион, минимальная зарплата, категории.\n"
    "🔑 Ключевые слова — добавить своё или отсечь лишнее.\n"
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_new = db.ensure_user(user.id, update.effective_chat.id, user.username or "")
    await update.message.reply_text(
        WELCOME.format(interval=CHECK_INTERVAL_MINUTES),
        parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU,
    )
    if is_new:
        db.mark_all_sent(user.id)  # чтобы не завалить историей
        await update.message.reply_text("Собираю первую подборку, это займёт полминуты…")
        await collect()
        db.forget_sent(user.id)
        st = db.get_settings(user.id)
        picks = pick_for_user(st, limit=5)
        db.mark_all_sent(user.id)
        if picks:
            await send_vacancies(context.bot, update.effective_chat.id, picks)
            await update.message.reply_text(
                "Это самое свежее. Дальше буду присылать только новое."
            )
        else:
            await update.message.reply_text(
                "Сейчас свежих подходящих вакансий нет — пришлю, как появятся."
            )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>Что умею</b>\n\n"
        "🔍 <b>Найти сейчас</b> — внеочередная проверка источников.\n"
        "⚙️ <b>Настройки</b> — регион, только удалёнка, минимальная зарплата, "
        "категории (UX / CX / Product / Marketing / соц. исследования), источники, "
        "режим (сразу или дайджест утром).\n"
        "🔑 <b>Ключевые слова</b> — свои слова в дополнение к встроенным и стоп-слова.\n"
        "⏸ <b>Пауза</b> — временно перестать присылать.\n\n"
        "<b>Команды</b>\n"
        "/search текст — разовый поиск по hh.ru (например: /search service design)\n"
        "/reset — снова показать вакансии, которые уже приходили\n"
        "/stats — статистика\n\n"
        "Кнопка «🚫 Скрыть компанию» под вакансией убирает из ленты все вакансии "
        "этого работодателя.",
        parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU,
    )


async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.ensure_user(update.effective_user.id, update.effective_chat.id,
                   update.effective_user.username or "")
    note = await update.message.reply_text("Проверяю источники…")
    st = db.get_settings(update.effective_user.id)
    await collect(set((st.get("sources") or db.DEFAULT_SOURCES).split(",")))
    picks = pick_for_user(st, limit=int(st.get("max_per_run", 8)))
    await note.delete()
    if not picks:
        await update.message.reply_text(
            "Нового нет. Как только появится — пришлю сам.", reply_markup=MAIN_MENU)
        return
    sent_uids = await send_vacancies(context.bot, update.effective_chat.id, picks)
    db.mark_sent(update.effective_user.id, sent_uids)


async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args or []).strip()
    if not query:
        await update.message.reply_text(
            "Напиши запрос после команды, например:\n/search service design")
        return
    note = await update.message.reply_text("Ищу «" + query + "» на hh.ru…")
    items = await search.free_search(query, period=30, limit=10)
    await note.delete()
    if not items:
        await update.message.reply_text("Ничего не нашлось.")
        return
    for vac in items:
        vac["categories"] = ",".join(matching.classify(vac["title"]) or [])
        text, keyboard = format_vacancy(vac)
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text=text, reply_markup=keyboard,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await asyncio.sleep(0.3)


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.set_setting(update.effective_user.id, "paused", 1)
    await update.message.reply_text(
        "⏸ Поставил на паузу. Включить обратно — /resume или кнопка «▶️ Включить».",
        reply_markup=MAIN_MENU)


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.set_setting(update.effective_user.id, "paused", 0)
    await update.message.reply_text("▶️ Снова слежу за вакансиями.", reply_markup=MAIN_MENU)


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.forget_sent(update.effective_user.id)
    await update.message.reply_text(
        "Историю отправок очистил — при следующей проверке пришлю всё подходящее заново.",
        reply_markup=MAIN_MENU)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = db.user_stats(update.effective_user.id)
    st = db.get_settings(update.effective_user.id)
    by_source = ", ".join(
        SOURCE_NAMES.get(name, name) + ": " + str(count) for name, count in s["by_source"]
    ) or "—"
    await update.message.reply_text(
        "📊 <b>Статистика</b>\n\n"
        "Отправлено за неделю: <b>" + str(s["sent_week"]) + "</b>\n"
        "Отправлено всего: " + str(s["sent_total"]) + "\n"
        "Подходящих вакансий в базе: " + str(s["in_base"]) + "\n"
        "Появилось за сутки: " + str(s["fresh_day"]) + "\n"
        "По источникам: " + by_source + "\n\n"
        "Режим: " + ("дайджест в " + str(st["digest_hour"]) + ":00"
                     if st["mode"] == "digest" else "присылать сразу") + "\n"
        "Статус: " + ("на паузе ⏸" if st["paused"] else "работает ▶️"),
        parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU)


# ------------------------------------------------------------- настройки

def settings_text(st):
    cats = [c for c in (st["categories"] or "").split(",") if c]
    cat_names = ", ".join(matching.category_label(c)[1] for c in cats) or "все"
    srcs = [s for s in (st["sources"] or "").split(",") if s]
    return (
        "⚙️ <b>Настройки</b>\n\n"
        "Регион: <b>" + REGIONS.get(st["region"], "Вся Россия") + "</b>\n"
        "Формат работы: <b>" + (format_names(st["work_formats"]) or "любой") + "</b>\n"
        "Минимальная зарплата: <b>" +
        (money(st["min_salary"], "RUR") if st["min_salary"] else "не важно") + "</b>\n"
        "Вакансии без зарплаты: <b>" +
        ("показывать" if st["include_no_salary"] else "скрывать") + "</b>\n"
        "Категории: <b>" + cat_names + "</b>\n"
        "Источники: <b>" + ", ".join(SOURCE_NAMES.get(s, s) for s in srcs) + "</b>\n"
        "Режим: <b>" + ("дайджест в " + str(st["digest_hour"]) + ":00"
                        if st["mode"] == "digest" else "присылать сразу") + "</b>\n"
        "Тишина: <b>с " + str(st["quiet_start"]) + ":00 до " + str(st["quiet_end"]) + ":00</b>"
    )


def settings_keyboard(st):
    cats = set(c for c in (st["categories"] or "").split(",") if c)
    srcs = set(s for s in (st["sources"] or "").split(",") if s)
    mark = lambda on: "✅ " if on else "▫️ "

    rows = [[InlineKeyboardButton(
        ("🔘 " if st["region"] == key else "") + name, callback_data="s:region:" + key)
        for key, name in REGIONS.items()]]
    formats = set(f for f in (st["work_formats"] or "").split(",") if f)
    rows.append([InlineKeyboardButton(mark(f in formats) + WORK_FORMAT[f],
                                      callback_data="wf:" + f)
                 for f in WORK_FORMAT_ORDER])
    rows.append([
        InlineKeyboardButton(mark(st["include_no_salary"]) + "вакансии без ЗП",
                             callback_data="s:include_no_salary:" +
                             str(0 if st["include_no_salary"] else 1)),
    ])
    rows.append([InlineKeyboardButton(
        "💰 от " + (money(st["min_salary"], "RUR") if st["min_salary"] else "любой"),
        callback_data="s:min_salary:next")])

    cat_row = []
    for cid in matching.ALL_CATEGORY_IDS:
        emoji, label = matching.category_label(cid)
        cat_row.append(InlineKeyboardButton(
            mark(cid in cats) + emoji + " " + label, callback_data="cat:" + cid))
    rows += [cat_row[i:i + 2] for i in range(0, len(cat_row), 2)]

    rows.append([InlineKeyboardButton(mark(s in srcs) + SOURCE_NAMES[s],
                                      callback_data="src:" + s)
                 for s in ["hh", "habr", "getmatch"]])
    rows.append([
        InlineKeyboardButton(("🔘 " if st["mode"] == "instant" else "") + "сразу",
                             callback_data="s:mode:instant"),
        InlineKeyboardButton(("🔘 " if st["mode"] == "digest" else "") + "дайджест утром",
                             callback_data="s:mode:digest"),
    ])
    return InlineKeyboardMarkup(rows)


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = db.get_settings(update.effective_user.id)
    await update.message.reply_text(settings_text(st), parse_mode=ParseMode.HTML,
                                    reply_markup=settings_keyboard(st))


async def cmd_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    includes = db.get_keywords(user_id, "include")
    excludes = db.get_keywords(user_id, "exclude")
    muted = db.get_muted(user_id)
    await update.message.reply_text(
        "🔑 <b>Ключевые слова</b>\n\n"
        "Встроенный список уже ловит: UX / CX / Product / Market Research, "
        "researcher, ресерч, исследователь, исследования, социолог, юзабилити, "
        "CustDev, глубинные интервью, фокус-группы, исследователь пользовательского "
        "и клиентского опыта.\n\n"
        "➕ Свои слова: <b>" + (", ".join(includes) or "нет") + "</b>\n"
        "🚫 Стоп-слова: <b>" + (", ".join(excludes) or "нет") + "</b>\n"
        "🏢 Скрытые компании: <b>" + (", ".join(muted) or "нет") + "</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить слово", callback_data="kw:add:include"),
             InlineKeyboardButton("🚫 Добавить стоп-слово", callback_data="kw:add:exclude")],
            [InlineKeyboardButton("🗑 Убрать слово", callback_data="kw:del:include"),
             InlineKeyboardButton("🗑 Убрать стоп-слово", callback_data="kw:del:exclude")],
            [InlineKeyboardButton("🏢 Вернуть все компании", callback_data="kw:unmute:all")],
        ]))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""
    db.ensure_user(user_id, query.message.chat_id, query.from_user.username or "")

    if data.startswith("mute:"):
        uid = data.split(":", 1)[1]
        vac = next((v for v in db.unsent_for_user(user_id, days=60) if v["uid"] == uid), None)
        company = (vac or {}).get("company") or ""
        if not company:
            conn = db.get_conn()
            row = conn.execute("SELECT company FROM vacancies WHERE uid = ?", (uid,)).fetchone()
            conn.close()
            company = row["company"] if row else ""
        if company:
            db.mute_company(user_id, company)
            await query.answer("Скрыл вакансии компании «" + company + "»", show_alert=False)
        else:
            await query.answer("Не нашёл компанию")
        return

    if data.startswith("kw:"):
        _, action, target = data.split(":", 2)
        if action == "unmute":
            for company in db.get_muted(user_id):
                db.unmute_company(user_id, company)
            await query.answer("Компании вернул")
            await query.edit_message_reply_markup(reply_markup=None)
            return
        context.user_data["awaiting"] = (action, target)
        prompt = {
            ("add", "include"): "Напиши слово, которое нужно ловить дополнительно "
                                "(например: аналитик или service design).",
            ("add", "exclude"): "Напиши слово, вакансии с которым присылать не нужно "
                                "(например: стажёр).",
            ("del", "include"): "Какое своё слово убрать? Сейчас: " +
                                (", ".join(db.get_keywords(user_id, "include")) or "пусто"),
            ("del", "exclude"): "Какое стоп-слово убрать? Сейчас: " +
                                (", ".join(db.get_keywords(user_id, "exclude")) or "пусто"),
        }[(action, target)]
        await query.answer()
        await query.message.reply_text(prompt)
        return

    st = db.get_settings(user_id)
    if data.startswith("s:"):
        _, key, value = data.split(":", 2)
        if key == "min_salary":
            current = int(st["min_salary"] or 0)
            nxt = SALARY_STEPS[(SALARY_STEPS.index(current) + 1) % len(SALARY_STEPS)] \
                if current in SALARY_STEPS else 0
            db.set_setting(user_id, "min_salary", nxt)
        else:
            db.set_setting(user_id, key, int(value) if value.isdigit() else value)
    elif data.startswith("wf:"):
        fid = data.split(":", 1)[1]
        formats = [f for f in (st["work_formats"] or "").split(",") if f]
        formats = [f for f in formats if f != fid] if fid in formats else formats + [fid]
        db.set_setting(user_id, "work_formats", ",".join(formats))
    elif data.startswith("cat:"):
        cid = data.split(":", 1)[1]
        cats = [c for c in (st["categories"] or "").split(",") if c]
        cats = [c for c in cats if c != cid] if cid in cats else cats + [cid]
        db.set_setting(user_id, "categories", ",".join(cats))
    elif data.startswith("src:"):
        sid = data.split(":", 1)[1]
        srcs = [s for s in (st["sources"] or "").split(",") if s]
        srcs = [s for s in srcs if s != sid] if sid in srcs else srcs + [sid]
        db.set_setting(user_id, "sources", ",".join(srcs))

    st = db.get_settings(user_id)
    await query.answer("Сохранил")
    try:
        await query.edit_message_text(settings_text(st), parse_mode=ParseMode.HTML,
                                      reply_markup=settings_keyboard(st))
    except TelegramError:
        pass


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id
    db.ensure_user(user_id, update.effective_chat.id, update.effective_user.username or "")

    awaiting = context.user_data.pop("awaiting", None)
    if awaiting:
        action, target = awaiting
        word = text.lower().strip()
        if action == "add":
            db.add_keyword(user_id, target, word)
            await update.message.reply_text(
                ("Добавил в свои слова: " if target == "include" else "Добавил в стоп-слова: ")
                + word, reply_markup=MAIN_MENU)
        else:
            removed = db.remove_keyword(user_id, target, word)
            await update.message.reply_text(
                "Убрал: " + word if removed else "Такого слова не было: " + word,
                reply_markup=MAIN_MENU)
        return

    if text == "🔍 Найти сейчас":
        await cmd_find(update, context)
    elif text == "📊 Статистика":
        await cmd_stats(update, context)
    elif text == "⚙️ Настройки":
        await cmd_settings(update, context)
    elif text == "🔑 Ключевые слова":
        await cmd_keywords(update, context)
    elif text == "❓ Помощь":
        await cmd_help(update, context)
    elif text in ("⏸ Пауза", "▶️ Включить"):
        st = db.get_settings(user_id)
        if st["paused"]:
            await cmd_resume(update, context)
        else:
            await cmd_pause(update, context)
    else:
        await update.message.reply_text(
            "Не понял. Нажми кнопку меню или напиши /help.\n"
            "Разовый поиск: /search текст запроса", reply_markup=MAIN_MENU)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_ID or str(update.effective_user.id) != str(ADMIN_ID):
        return
    total, active = db.all_users_count()
    s = db.user_stats(update.effective_user.id)
    await update.message.reply_text(
        "👤 Пользователей: " + str(total) + " (активных " + str(active) + ")\n"
        "📥 Вакансий в базе: " + str(s["in_base"]) + ", за сутки: " + str(s["fresh_day"]) + "\n"
        "⏱ Интервал проверки: " + str(CHECK_INTERVAL_MINUTES) + " мин")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обрывы связи с Telegram - обычное дело, их логируем одной строкой.

    Провайдеры любят рвать TLS-рукопожатие до api.telegram.org; библиотека
    сама повторит запрос, поэтому простыня трейсбека тут ничего не даёт.
    """
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning("связь с Telegram оборвалась (%s), повторю позже",
                       type(err).__name__)
        return
    logger.error("не смог обработать обновление", exc_info=err)


async def _post_init(app: Application):
    asyncio.create_task(poll_loop(app))
    await app.bot.set_my_commands([
        ("start", "запустить радар"),
        ("find", "проверить прямо сейчас"),
        ("search", "разовый поиск по hh.ru"),
        ("settings", "настройки"),
        ("keywords", "ключевые слова"),
        ("stats", "статистика"),
        ("pause", "пауза"),
        ("resume", "продолжить"),
        ("help", "справка"),
    ])


def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "Не найден BOT_TOKEN. Скопируй .env.example в .env и вставь токен от "
            "@BotFather (или задай переменную BOT_TOKEN в панели хостинга)."
        )

    db.init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(20)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(20)
        .get_updates_connect_timeout(20)
        .get_updates_read_timeout(35)
        .post_init(_post_init)
        .build()
    )
    app.add_error_handler(on_error)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("find", cmd_find))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("keywords", cmd_keywords))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Бот запущен. Останови через Ctrl+C.")
    app.run_polling()


if __name__ == "__main__":
    main()
