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

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

MSK = timezone(timedelta(hours=3))

DB_PATH = Path(os.getenv("DB_PATH") or (Path(__file__).parent / "jobs.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_CATEGORIES = "ux,cx,product,marketing,socio,research"
DEFAULT_SOURCES = "hh,habr,getmatch,geekjob,itone,superjob,tg,linkedin,companies"

DEFAULT_WORK_FORMATS = "remote"     # remote | hybrid | office, через запятую

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
    "max_per_run": 8,
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
            max_per_run INTEGER DEFAULT 8,
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
            first_seen TEXT
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
    conn.close()


def _migrate(conn):
    """Дописывает колонки, появившиеся после первого запуска бота."""
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
        "max_per_run": "INTEGER DEFAULT 8",
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

def upsert_vacancies(items):
    """Записывает вакансии, возвращает список тех, которых раньше не было."""
    conn = get_conn()
    stamp = now().isoformat(timespec="seconds")
    recent = (now() - timedelta(days=14)).isoformat(timespec="seconds")
    fresh = []
    for v in items:
        exists = conn.execute(
            "SELECT 1 FROM vacancies WHERE uid = ?", (v["uid"],)).fetchone()
        if exists:
            continue
        # один и тот же текст вакансии часто висит несколькими объявлениями;
        # у удалёнки её ещё и размножают по городам - там город игнорируем
        if "remote" in (v.get("work_format") or ""):
            twin = conn.execute(
                "SELECT 1 FROM vacancies WHERE title = ? AND company = ?"
                " AND published_at >= ?", (v["title"], v["company"], recent)).fetchone()
        else:
            twin = conn.execute(
                "SELECT 1 FROM vacancies WHERE title = ? AND company = ? AND area = ?"
                " AND published_at >= ?",
                (v["title"], v["company"], v["area"], recent)).fetchone()
        if twin:
            continue
        conn.execute(
            "INSERT INTO vacancies (uid, source, ext_id, title, company, area, url,"
            " published_at, salary_from, salary_to, currency, work_format, experience,"
            " categories, relevant, first_seen)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (v["uid"], v["source"], v["ext_id"], v["title"], v["company"], v["area"],
             v["url"], v["published_at"], v.get("salary_from"), v.get("salary_to"),
             v.get("currency", ""), v.get("work_format", ""), v.get("experience", ""),
             ",".join(v.get("categories") or []), 1 if v.get("relevant") else 0, stamp),
        )
        fresh.append(v)
    conn.commit()
    conn.close()
    return fresh


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


def unsent_for_user(user_id, days=30, limit=500):
    """Вакансии, которые этому человеку ещё не отправляли (свежие сверху)."""
    since = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    conn = get_conn()
    rows = conn.execute(
        "SELECT v.* FROM vacancies v"
        " LEFT JOIN sent s ON s.uid = v.uid AND s.user_id = ?"
        " WHERE s.uid IS NULL AND v.published_at >= ?"
        " ORDER BY v.published_at DESC LIMIT ?",
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
    title = (vac.get("title") or "").lower()

    for word in excludes:
        if word and word in title:
            return False

    company = (vac.get("company") or "").lower()
    if company and company in muted:
        return False

    cats = [c for c in (vac.get("categories") or "").split(",") if c]
    # пустой список в настройках читаем как «все» - иначе лента молчит
    wanted = [c for c in (st.get("categories") or "").split(",") if c] \
        or DEFAULT_CATEGORIES.split(",")
    rescued = any(word and word in title for word in includes)

    if not vac.get("relevant"):
        if not rescued:
            return False
    elif cats and not set(cats) & set(wanted):
        if not rescued:
            return False

    formats = set(f for f in (vac.get("work_format") or "").split(",") if f) or {"office"}
    area = (vac.get("area") or "").lower()

    wanted_formats = set(
        f for f in (st.get("work_formats") or DEFAULT_WORK_FORMATS).split(",") if f)
    # «удалёнка по договорённости» (в шапке гибрид, в описании обещают
    # удалёнку) засчитывается тому, кто просил удалёнку
    if "remote" in wanted_formats:
        wanted_formats.add("remote_maybe")
    if wanted_formats and not formats & wanted_formats:
        return False

    region = st.get("region") or "any"
    if region in MSK_CITY and not formats & {"remote", "remote_maybe"}:
        if not any(city in area for city in MSK_CITY[region]):
            return False

    s_from = vac.get("salary_from") or 0
    s_to = vac.get("salary_to") or 0
    min_salary = st.get("min_salary") or 0
    if min_salary:
        top = max(s_from, s_to)
        if not top:
            if not st.get("include_no_salary"):
                return False
        elif top < min_salary:
            return False

    return True
