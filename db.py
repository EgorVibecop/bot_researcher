"""
Хранилище бота: SQLite.

Таблицы:
  users      - подписчики и их настройки (одна строка на человека)
  vacancies  - все собранные вакансии с тегами категорий
  sent       - что кому уже отправлено (чтобы не слать дважды)
  keywords   - личные ключевые слова: kind = 'include' | 'exclude'
  muted      - компании, которые пользователь скрыл

Путь к базе можно переопределить переменной DB_PATH - нужно на хостинге,
чтобы положить базу на постоянный диск и не терять историю при передеплое.
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matching

MSK = timezone(timedelta(hours=3))

DB_PATH = Path(os.getenv("DB_PATH") or (Path(__file__).parent / "jobs.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Категории и формат работы зависят от профиля: у исследователя одни,
# у аналитика другие (и офиса в списке нет вовсе).
DEFAULT_CATEGORIES = ",".join(matching.ALL_CATEGORY_IDS)
DEFAULT_SOURCES = ("hh,zarplata,rabota,trudvsem,habr,getmatch,geekjob,itone,superjob,"
                   "tg,linkedin,companies")

DEFAULT_WORK_FORMATS = matching.DEFAULT_WORK_FORMATS

SETTINGS_DEFAULTS = {
    "region": "any",            # any | msk | spb
    "work_formats": DEFAULT_WORK_FORMATS,
    "min_salary": 0,
    "include_no_salary": 1,
    "categories": DEFAULT_CATEGORIES,
    "sources": DEFAULT_SOURCES,
    # Храним не список включённых источников, а список выключенных вручную.
    # Иначе новый источник не доходит до тех, кто завёлся раньше: у них в
    # строке остаётся старый список, и добавленный LinkedIn просто не
    # опрашивается.
    "sources_off": "",
    "mode": "instant",          # instant | digest
    "digest_hour": 10,
    "quiet_start": 23,
    "quiet_end": 8,
    "max_per_run": 5,
    "only_ru": 1,
    "paused": 0,
}

MSK_CITY = {
    "msk": ("москва", "московская"),
    "spb": ("санкт-петербург", "петербург", "ленинградская"),
}


def now():
    return datetime.now(MSK)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            username TEXT,
            created_at TEXT,
            last_seen TEXT,
            region TEXT DEFAULT 'any',
            work_formats TEXT DEFAULT 'remote',
            min_salary INTEGER DEFAULT 0,
            include_no_salary INTEGER DEFAULT 1,
            categories TEXT,
            sources TEXT,
            sources_off TEXT DEFAULT '',
            mode TEXT DEFAULT 'instant',
            digest_hour INTEGER DEFAULT 10,
            quiet_start INTEGER DEFAULT 23,
            quiet_end INTEGER DEFAULT 8,
            max_per_run INTEGER DEFAULT 5,
            only_ru INTEGER DEFAULT 1,
            paused INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS vacancies (
            uid TEXT PRIMARY KEY,
            source TEXT,
            ext_id TEXT,
            title TEXT,
            company TEXT,
            area TEXT,
            url TEXT,
            published_at TEXT,
            salary_from INTEGER,
            salary_to INTEGER,
            currency TEXT,
            work_format TEXT,
            experience TEXT,
            categories TEXT,
            relevant INTEGER DEFAULT 0,
            remote_checked INTEGER DEFAULT 0,
            closed INTEGER DEFAULT 0,
            remote_note TEXT,
            it_accred TEXT,
            first_seen TEXT
        );

        CREATE TABLE IF NOT EXISTS employer_accred (
            name TEXT PRIMARY KEY,
            accred TEXT,
            checked_at TEXT
        );

        CREATE TABLE IF NOT EXISTS sent (
            user_id INTEGER NOT NULL,
            uid TEXT NOT NULL,
            sent_at TEXT,
            PRIMARY KEY (user_id, uid)
        );

        CREATE TABLE IF NOT EXISTS keywords (
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            word TEXT NOT NULL,
            PRIMARY KEY (user_id, kind, word)
        );

        CREATE TABLE IF NOT EXISTS muted (
            user_id INTEGER NOT NULL,
            company TEXT NOT NULL,
            PRIMARY KEY (user_id, company)
        );

        CREATE INDEX IF NOT EXISTS idx_vac_pub ON vacancies(published_at);
        CREATE INDEX IF NOT EXISTS idx_vac_rel ON vacancies(relevant);
        """
    )
    _migrate(conn)
    conn.commit()
    counts = {name: conn.execute("SELECT COUNT(*) c FROM " + name).fetchone()["c"]
              for name in ("users", "vacancies", "sent")}
    conn.close()
    logging.getLogger(__name__).info(
        "база %s: пользователей %s, вакансий %s, записей об отправках %s",
        DB_PATH, counts["users"], counts["vacancies"], counts["sent"])


def _once(conn, name, sql, args=()):
    """Разовая правка данных: выполняется один раз за всю жизнь базы.

    Раньше такие UPDATE'ы стояли прямо в миграции и прогонялись при каждом
    запуске - то есть при каждом перезапуске сервиса настройки человека
    затирались обратно. Теперь имя правки запоминается.
    """
    conn.execute("CREATE TABLE IF NOT EXISTS applied (name TEXT PRIMARY KEY,"
                 " applied_at TEXT)")
    done = conn.execute("SELECT 1 FROM applied WHERE name = ?", (name,)).fetchone()
    if done:
        return
    conn.execute(sql, args)
    conn.execute("INSERT OR REPLACE INTO applied (name, applied_at) VALUES (?, ?)",
                 (name, now().isoformat(timespec="seconds")))


def _migrate(conn):
    """Дописывает колонки, появившиеся после первого запуска бота."""
    vac_have = {row["name"] for row in conn.execute("PRAGMA table_info(vacancies)")}
    if "remote_checked" not in vac_have:
        conn.execute("ALTER TABLE vacancies ADD COLUMN remote_checked INTEGER DEFAULT 0")
    if "closed" not in vac_have:
        conn.execute("ALTER TABLE vacancies ADD COLUMN closed INTEGER DEFAULT 0")
    if "remote_note" not in vac_have:
        conn.execute("ALTER TABLE vacancies ADD COLUMN remote_note TEXT")
    if "it_accred" not in vac_have:
        conn.execute("ALTER TABLE vacancies ADD COLUMN it_accred TEXT")

    # Раньше за отправку уходила порция в 8-15 вакансий, и лента выглядела
    # скупой при полной очереди. Переводим тех, кто размер не менял, на
    # «все»: 0 значит отдавать всё подходящее сразу.
    # Темп ленты: по пять вакансий в час. «Все разом» было временной мерой,
    # пока разбирались, почему приходит мало.
    _once(conn, "batch-5",
          "UPDATE users SET max_per_run = 5 WHERE max_per_run IN (0, 8, 15)")

    # Фильтры по зарплате, региону и категориям убраны из настроек: они
    # молча резали ленту (порог в 300 тысяч отсекал почти всё). Сбрасываем
    # их у тех, кто успел выставить.
    _once(conn, "drop-legacy-filters",
          "UPDATE users SET min_salary = 0, region = 'any',"
          " include_no_salary = 1, categories = ?", (DEFAULT_CATEGORIES,))

    # Полный офис убран из фильтра: у тех, кто его включал, оставляем
    # удалёнку и гибрид.
    _once(conn, "formats-remote-hybrid",
          "UPDATE users SET work_formats = 'remote,hybrid'"
          " WHERE IFNULL(work_formats, '') <> 'remote,hybrid'")

    # Цитаты, вырезанные из разметки страницы, нечитаемы: чистим один раз,
    # при следующей проверке описания бот подставит нормальную фразу.
    conn.execute("UPDATE vacancies SET remote_note = NULL, remote_checked = 0"
                 " WHERE remote_note LIKE '%\"%' OR remote_note LIKE '%{%'")

    have = {row["name"] for row in conn.execute("PRAGMA table_info(users)")}
    columns = {
        "work_formats": "TEXT DEFAULT 'remote'",
        "region": "TEXT DEFAULT 'any'",
        "min_salary": "INTEGER DEFAULT 0",
        "include_no_salary": "INTEGER DEFAULT 1",
        "categories": "TEXT",
        "sources": "TEXT",
        "sources_off": "TEXT DEFAULT ''",
        "mode": "TEXT DEFAULT 'instant'",
        "digest_hour": "INTEGER DEFAULT 10",
        "quiet_start": "INTEGER DEFAULT 23",
        "quiet_end": "INTEGER DEFAULT 8",
        "max_per_run": "INTEGER DEFAULT 5",
        "only_ru": "INTEGER DEFAULT 1",
        "paused": "INTEGER DEFAULT 0",
    }
    for name, decl in columns.items():
        if name not in have:
            conn.execute("ALTER TABLE users ADD COLUMN " + name + " " + decl)


# ------------------------------------------------------------------ пользователи

def ensure_user(user_id, chat_id, username):
    """Заводит пользователя. Возвращает True, если он тут впервые."""
    conn = get_conn()
    row = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)).fetchone()
    stamp = now().isoformat(timespec="seconds")
    if row is None:
        conn.execute(
            "INSERT INTO users (user_id, chat_id, username, created_at, last_seen,"
            " categories, sources, work_formats) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, chat_id, username, stamp, stamp, DEFAULT_CATEGORIES,
             DEFAULT_SOURCES, DEFAULT_WORK_FORMATS),
        )
        is_new = True
    else:
        conn.execute(
            "UPDATE users SET last_seen = ?, chat_id = ?, username = ? WHERE user_id = ?",
            (stamp, chat_id, username, user_id),
        )
        is_new = False
    conn.commit()
    conn.close()
    return is_new


def get_settings(user_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    if row is None:
        return dict(SETTINGS_DEFAULTS, user_id=user_id, chat_id=user_id)
    st = dict(row)
    for key, default in SETTINGS_DEFAULTS.items():
        if st.get(key) in (None, ""):
            st[key] = default
    return st


def active_sources(st):
    """Все источники, кроме выключенных вручную.

    Считаем от текущего списка DEFAULT_SOURCES, поэтому добавленный источник
    сразу работает у всех, а не только у тех, кто завёлся после него.
    """
    off = {s for s in (st.get("sources_off") or "").split(",") if s}
    return [s for s in DEFAULT_SOURCES.split(",") if s not in off]


def toggle_source(user_id, source):
    """Включает или выключает один источник, возвращает новое состояние."""
    st = get_settings(user_id)
    off = [s for s in (st.get("sources_off") or "").split(",") if s]
    if source in off:
        off = [s for s in off if s != source]
        enabled = True
    else:
        off.append(source)
        enabled = False
    set_setting(user_id, "sources_off", ",".join(off))
    return enabled


def set_setting(user_id, key, value):
    if key not in SETTINGS_DEFAULTS:
        raise ValueError("неизвестная настройка: " + key)
    conn = get_conn()
    conn.execute("UPDATE users SET " + key + " = ? WHERE user_id = ?", (value, user_id))
    conn.commit()
    conn.close()


def active_users():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM users WHERE paused = 0").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def all_users_count():
    conn = get_conn()
    row = conn.execute(
        "SELECT COUNT(*) c, SUM(paused = 0) a FROM users").fetchone()
    conn.close()
    return row["c"] or 0, row["a"] or 0


# --------------------------------------------------------------------- вакансии

# Сайты самих компаний: их ссылка важнее ссылки агрегатора.
OWN_SITE_SOURCES = {"careers", "magnit", "avito", "2gis", "dodo", "aviasales", "itone"}


def upsert_vacancies(items):
    """Записывает вакансии, возвращает список тех, которых раньше не было."""
    conn = get_conn()
    stamp = now().isoformat(timespec="seconds")
    recent = (now() - timedelta(days=14)).isoformat(timespec="seconds")
    fresh = []
    for v in items:
        exists = conn.execute(
            "SELECT relevant, categories FROM vacancies WHERE uid = ?",
            (v["uid"],)).fetchone()
        if exists:
            # Фильтр со временем умнеет, а вердикт у старых записей остаётся
            # прежним - и починка стоп-слова до человека не доезжает. Поэтому
            # переоцениваем то, что уже лежит в базе.
            was_relevant = bool(exists["relevant"])
            now_relevant = bool(v.get("relevant"))
            cats = ",".join(v.get("categories") or [])
            if was_relevant != now_relevant or (exists["categories"] or "") != cats:
                conn.execute(
                    "UPDATE vacancies SET relevant = ?, categories = ? WHERE uid = ?",
                    (1 if now_relevant else 0, cats, v["uid"]))
            continue
        # один и тот же текст вакансии часто висит несколькими объявлениями;
        # у удалёнки её ещё и размножают по городам - там город игнорируем
        if "remote" in (v.get("work_format") or ""):
            twin = conn.execute(
                "SELECT uid, source, url FROM vacancies WHERE title = ? AND company = ?"
                " AND published_at >= ?", (v["title"], v["company"], recent)).fetchone()
        else:
            twin = conn.execute(
                "SELECT uid, source, url FROM vacancies WHERE title = ? AND company = ?"
                " AND area = ? AND published_at >= ?",
                (v["title"], v["company"], v["area"], recent)).fetchone()
        if twin:
            # Дубль не заводим, но если он пришёл с сайта самой компании, а в
            # базе лежит версия с агрегатора - подменяем ссылку. Человеку
            # нужен работодатель, а не посредник.
            if (v.get("source") in OWN_SITE_SOURCES
                    and twin["source"] not in OWN_SITE_SOURCES):
                conn.execute("UPDATE vacancies SET source = ?, url = ? WHERE uid = ?",
                             (v["source"], v["url"], twin["uid"]))
            continue
        conn.execute(
            "INSERT INTO vacancies (uid, source, ext_id, title, company, area, url,"
            " published_at, salary_from, salary_to, currency, work_format, experience,"
            " categories, relevant, first_seen, remote_checked, remote_note, it_accred)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (v["uid"], v["source"], v["ext_id"], v["title"], v["company"], v["area"],
             v["url"], v["published_at"], v.get("salary_from"), v.get("salary_to"),
             v.get("currency", ""), v.get("work_format", ""), v.get("experience", ""),
             ",".join(v.get("categories") or []), 1 if v.get("relevant") else 0, stamp,
             1 if v.get("remote_checked") else 0, v.get("remote_note"),
             v.get("it_accred")),
        )
        fresh.append(v)
    conn.commit()
    conn.close()
    return fresh


def reclassify_all(classify):
    """Перепроверяет фильтром все вакансии в базе, а не только свежие.

    Раньше вердикт обновлялся лишь у тех, что попались в текущей выдаче.
    Вакансия, выпавшая из поиска (кончилось окно, не подошла под запрос),
    навсегда оставалась с устаревшим вердиктом - и починенное стоп-слово
    до человека не доезжало. Проверка локальная, сети не трогает.
    """
    conn = get_conn()
    rows = conn.execute("SELECT uid, title, relevant, categories FROM vacancies").fetchall()
    changes = []
    for row in rows:
        cats = classify(row["title"])
        relevant = 1 if cats is not None else 0
        joined = ",".join(cats or [])
        if relevant != (row["relevant"] or 0) or joined != (row["categories"] or ""):
            changes.append((relevant, joined, row["uid"]))
    if changes:
        conn.executemany(
            "UPDATE vacancies SET relevant = ?, categories = ? WHERE uid = ?", changes)
        conn.commit()
    conn.close()
    return len(changes)


def mark_closed(uids):
    """Помечает вакансии снятыми - в ленту они больше не попадут."""
    if not uids:
        return
    conn = get_conn()
    conn.executemany("UPDATE vacancies SET closed = 1 WHERE uid = ?",
                     [(uid,) for uid in uids])
    conn.commit()
    conn.close()


def close_missing(source, alive_uids):
    """Закрывает вакансии источника, которых больше нет в его выдаче.

    Годится только для источников, отдающих полный каталог (getmatch,
    Авито, 2ГИС, Dodo, Aviasales). Для поисковых выдач вроде hh так делать
    нельзя: там вакансия выпадает из ответа просто потому, что не подошла
    под запрос или вышла из окна поиска.
    """
    if not alive_uids:
        return 0
    conn = get_conn()
    alive = list(alive_uids)
    marks = ",".join("?" * len(alive))
    cur = conn.execute(
        "UPDATE vacancies SET closed = 1 WHERE source = ? AND IFNULL(closed, 0) = 0"
        " AND uid NOT IN (" + marks + ")", [source] + alive)
    conn.commit()
    closed = cur.rowcount
    conn.close()
    return closed


def unchecked_remote(limit=15, days=21):
    """Подходящие вакансии без удалёнки, у которых мы ещё не читали описание.

    Нужно для накопленного: обогащение формата идёт только по свежим
    находкам, а весь LinkedIn уже лежал в базе с пустым форматом и из-за
    этого не доезжал до тех, у кого стоит «только удалёнка».
    """
    since = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    rows = conn.execute(
        "SELECT uid, source, ext_id, url, work_format, it_accred FROM vacancies"
        " WHERE relevant = 1 AND IFNULL(remote_checked, 0) = 0"
        " AND published_at >= ?"
        # Сначала те, у кого формат вообще не указан - это LinkedIn и другие
        # источники, которые его не публикуют. У них шанс найти удалёнку в
        # тексте высокий, а у офисной вакансии с hh - низкий.
        " ORDER BY CASE WHEN IFNULL(work_format, '') = '' THEN 0 ELSE 1 END,"
        " published_at DESC LIMIT ?",
        (since, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_formats(items):
    """Сохраняет формат после чтения описания и отмечает вакансию проверенной."""
    if not items:
        return
    conn = get_conn()
    conn.executemany(
        "UPDATE vacancies SET work_format = ?, remote_checked = 1, remote_note = ?,"
        " it_accred = COALESCE(NULLIF(?, ''), it_accred) WHERE uid = ?",
        [(v.get("work_format") or "", v.get("remote_note"), v.get("it_accred"),
          v["uid"]) for v in items])
    conn.commit()
    conn.close()


def known_uids(uids):
    """Какие из этих вакансий уже лежат в базе — чтобы не перечитывать их."""
    if not uids:
        return set()
    conn = get_conn()
    found = set()
    uids = list(uids)
    for start in range(0, len(uids), 400):
        chunk = uids[start:start + 400]
        marks = ",".join("?" * len(chunk))
        rows = conn.execute(
            "SELECT uid FROM vacancies WHERE uid IN (" + marks + ")", chunk).fetchall()
        found |= {r["uid"] for r in rows}
    conn.close()
    return found


def unsent_for_user(user_id, days=21, limit=20000):
    """Вакансии, которые этому человеку ещё не отправляли.

    Сначала свежие. Потолок тут держим высоким: hh за один день выдаёт по
    широкому запросу больше пятисот вакансий, и при маленьком лимите всё,
    что старше сегодняшнего дня, не попадало в очередь никогда.
    """
    since = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    rows = conn.execute(
        "SELECT v.* FROM vacancies v"
        " LEFT JOIN sent s ON s.uid = v.uid AND s.user_id = ?"
        # У части источников даты публикации нет вовсе (IT-One, карьерные
        # сайты, телеграм). Раньше сравнение с NULL было ложным, и такие
        # вакансии не доезжали никогда - считаем их по дате находки.
        " WHERE s.uid IS NULL AND IFNULL(v.published_at, v.first_seen) >= ?"
        " AND IFNULL(v.closed, 0) = 0"
        " ORDER BY IFNULL(v.published_at, v.first_seen) DESC LIMIT ?",
        (user_id, since, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def recent_relevant(days=14, limit=300):
    since = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM vacancies WHERE relevant = 1 AND published_at >= ?"
        " ORDER BY published_at DESC LIMIT ?", (since, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def accred_cached(name):
    """Что мы уже знаем про аккредитацию этой компании: 'yes', '' или None.

    None значит «не спрашивали», пустая строка - «спрашивали, ответа нет»:
    второй раз ходить за тем же не нужно.
    """
    conn = get_conn()
    row = conn.execute("SELECT accred FROM employer_accred WHERE name = ?",
                       (name.strip().lower(),)).fetchone()
    conn.close()
    return None if row is None else (row["accred"] or "")


def remember_accred(name, accred):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO employer_accred (name, accred, checked_at)"
        " VALUES (?, ?, ?)",
        (name.strip().lower(), accred or "", now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()


def mark_sent(user_id, uids):
    if not uids:
        return
    conn = get_conn()
    stamp = now().isoformat(timespec="seconds")
    conn.executemany(
        "INSERT OR IGNORE INTO sent (user_id, uid, sent_at) VALUES (?, ?, ?)",
        [(user_id, uid, stamp) for uid in uids],
    )
    conn.commit()
    conn.close()


def mark_all_sent(user_id):
    """Считает всё уже накопленное отправленным - чтобы новичка не завалило."""
    conn = get_conn()
    stamp = now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT OR IGNORE INTO sent (user_id, uid, sent_at)"
        " SELECT ?, uid, ? FROM vacancies", (user_id, stamp))
    conn.commit()
    conn.close()


def forget_sent(user_id):
    conn = get_conn()
    conn.execute("DELETE FROM sent WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def forget_sent_all():
    """Сбрасывает историю отправок у всех. Нужно после правки фильтра,
    чтобы люди увидели то, что бот раньше молча отсеивал."""
    conn = get_conn()
    cur = conn.execute("DELETE FROM sent")
    conn.commit()
    count = cur.rowcount
    conn.close()
    return count


def cleanup(days=45):
    """Убирает старые вакансии и хвосты отправок."""
    edge = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    conn.execute("DELETE FROM vacancies WHERE published_at < ?", (edge,))
    conn.execute(
        "DELETE FROM sent WHERE uid NOT IN (SELECT uid FROM vacancies)")
    conn.commit()
    conn.close()


# ------------------------------------------------------------ ключевые слова

def add_keyword(user_id, kind, word):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO keywords (user_id, kind, word) VALUES (?, ?, ?)",
        (user_id, kind, word.strip().lower()))
    conn.commit()
    conn.close()


def remove_keyword(user_id, kind, word):
    conn = get_conn()
    cur = conn.execute(
        "DELETE FROM keywords WHERE user_id = ? AND kind = ? AND word = ?",
        (user_id, kind, word.strip().lower()))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return deleted


def get_keywords(user_id, kind):
    conn = get_conn()
    rows = conn.execute(
        "SELECT word FROM keywords WHERE user_id = ? AND kind = ? ORDER BY word",
        (user_id, kind)).fetchall()
    conn.close()
    return [r["word"] for r in rows]


def mute_company(user_id, company):
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO muted (user_id, company) VALUES (?, ?)",
                 (user_id, (company or "").strip().lower()))
    conn.commit()
    conn.close()


def unmute_company(user_id, company):
    conn = get_conn()
    conn.execute("DELETE FROM muted WHERE user_id = ? AND company = ?",
                 (user_id, (company or "").strip().lower()))
    conn.commit()
    conn.close()


def get_muted(user_id):
    conn = get_conn()
    rows = conn.execute("SELECT company FROM muted WHERE user_id = ? ORDER BY company",
                        (user_id,)).fetchall()
    conn.close()
    return [r["company"] for r in rows]


# ----------------------------------------------------------------- статистика

def funnel(user_id, days):
    """Сколько вакансий отсекается на каждом шаге - от базы до очереди.

    Нужна, чтобы на «почему приходит не всё» отвечать цифрами: по каждому
    шагу видно, сколько вакансий он съел.
    """
    since = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    one = lambda sql, args=(): conn.execute(sql, args).fetchone()["c"]
    out = {
        "всего в базе": one("SELECT COUNT(*) c FROM vacancies"),
        "по теме": one("SELECT COUNT(*) c FROM vacancies WHERE relevant = 1"),
        "свежее окна": one(
            "SELECT COUNT(*) c FROM vacancies WHERE relevant = 1"
            " AND IFNULL(published_at, first_seen) >= ?", (since,)),
        "не сняты с публикации": one(
            "SELECT COUNT(*) c FROM vacancies WHERE relevant = 1"
            " AND IFNULL(published_at, first_seen) >= ? AND IFNULL(closed, 0) = 0",
            (since,)),
        "ещё не отправляли вам": one(
            "SELECT COUNT(*) c FROM vacancies v"
            " LEFT JOIN sent s ON s.uid = v.uid AND s.user_id = ?"
            " WHERE s.uid IS NULL AND v.relevant = 1"
            " AND IFNULL(v.published_at, v.first_seen) >= ?"
            " AND IFNULL(v.closed, 0) = 0", (user_id, since)),
    }
    conn.close()
    return out


def user_stats(user_id):
    conn = get_conn()
    week = (now() - timedelta(days=7)).isoformat(timespec="seconds")
    day = (now() - timedelta(days=1)).isoformat(timespec="seconds")
    sent_week = conn.execute(
        "SELECT COUNT(*) c FROM sent WHERE user_id = ? AND sent_at >= ?",
        (user_id, week)).fetchone()["c"]
    sent_total = conn.execute(
        "SELECT COUNT(*) c FROM sent WHERE user_id = ?", (user_id,)).fetchone()["c"]
    in_base = conn.execute(
        "SELECT COUNT(*) c FROM vacancies WHERE relevant = 1").fetchone()["c"]
    fresh_day = conn.execute(
        "SELECT COUNT(*) c FROM vacancies WHERE relevant = 1 AND first_seen >= ?",
        (day,)).fetchone()["c"]
    by_source = conn.execute(
        "SELECT source, COUNT(*) c FROM vacancies WHERE relevant = 1"
        " GROUP BY source ORDER BY c DESC").fetchall()
    conn.close()
    return {
        "sent_week": sent_week,
        "sent_total": sent_total,
        "in_base": in_base,
        "fresh_day": fresh_day,
        "by_source": [(r["source"], r["c"]) for r in by_source],
    }


# ------------------------------------------------------------------- фильтры

def matches_user(vac, st, includes, excludes, muted):
    """Проходит ли вакансия личные фильтры пользователя."""
    return match_reason(vac, st, includes, excludes, muted) is None


# Источники, где попадаются вакансии других стран.
WORLDWIDE_SOURCES = {"linkedin", "tg"}


def work_bucket(formats):
    """Одна вакансия - одна корзина, без дублей в ленте.

    Есть удалёнка (хоть вместе с офисом и гибридом) - это «удалёнка».
    Нет удалёнки, но есть гибрид - «гибрид». Только офис - «офис».
    Источник промолчал - «unknown», такие показываем всем, иначе пропадёт
    весь LinkedIn, карьерные сайты и телеграм.
    """
    if not formats:
        return "unknown"
    if formats & {"remote", "remote_maybe"}:
        return "remote"
    if "hybrid" in formats:
        return "hybrid"
    if "office" in formats:
        return "office"
    return "unknown"


def match_reason(vac, st, includes, excludes, muted):
    """None, если вакансия подходит, иначе короткая причина отказа.

    Нужна, чтобы на вопрос «почему в очереди пусто» бот отвечал цифрами,
    а не догадками.
    """
    title = (vac.get("title") or "").lower()

    for word in excludes:
        if word and word in title:
            return "стоп-слово"

    company = (vac.get("company") or "").lower()
    if company and company in muted:
        return "скрытая компания"

    cats = [c for c in (vac.get("categories") or "").split(",") if c]
    # пустой список в настройках читаем как «все» - иначе лента молчит
    wanted = [c for c in (st.get("categories") or "").split(",") if c] \
        or DEFAULT_CATEGORIES.split(",")
    rescued = any(word and word in title for word in includes)

    if not vac.get("relevant"):
        if not rescued:
            return "не по теме"
    elif cats and not set(cats) & set(wanted):
        if not rescued:
            return "выключенная категория"

    # Пустой формат - это не офис, а «источник его не сообщил» (так приходит
    # весь LinkedIn). Разводим эти случаи, чтобы не выдавать догадку за факт.
    formats = set(f for f in (vac.get("work_format") or "").split(",") if f)
    area = (vac.get("area") or "").lower()

    wanted_formats = set(
        f for f in (st.get("work_formats") or DEFAULT_WORK_FORMATS).split(",") if f)
    bucket = work_bucket(formats)
    if wanted_formats and bucket != "unknown" and bucket not in wanted_formats:
        return "формат работы"

    # Зарубежные вакансии приходят только оттуда, где публикуют весь мир:
    # LinkedIn и телеграм-каналы. Российские площадки проверять незачем -
    # там и английское название вакансии про российский рынок.
    if (st.get("only_ru", 1) and vac.get("source") in WORLDWIDE_SOURCES
            and not matching.looks_russian(vac)):
        return "не про российский рынок"

    region = st.get("region") or "any"
    if region in MSK_CITY and not formats & {"remote", "remote_maybe"}:
        if not any(city in area for city in MSK_CITY[region]):
            return "регион"

    s_from = vac.get("salary_from") or 0
    s_to = vac.get("salary_to") or 0
    min_salary = st.get("min_salary") or 0
    if min_salary:
        top = max(s_from, s_to)
        if not top:
            if not st.get("include_no_salary"):
                return "без зарплаты"
        elif top < min_salary:
            return "зарплата ниже порога"

    return None
