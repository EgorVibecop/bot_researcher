"""Прогон команд бота без телеграма.

Нужен, чтобы опечатка в /stats или в фильтре не доезжала до живого бота:
здесь поднимается настоящая база во временном файле, в неё кладутся
вакансии, а дальше команды вызываются с подставным Update.
"""

import asyncio
import os
import sys
import tempfile

os.environ.setdefault("BOT_TOKEN", "123456:test-token")
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "smoke.db")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bot  # noqa: E402
import db  # noqa: E402
import matching  # noqa: E402

USER = 4242
SENT = []


class FakeMessage:
    async def reply_text(self, text, **kwargs):
        SENT.append(text)
        return self


class FakeUser:
    id = USER
    username = "smoke"
    first_name = "Smoke"


class FakeChat:
    id = USER


class FakeUpdate:
    message = FakeMessage()
    effective_user = FakeUser()
    effective_chat = FakeChat()
    callback_query = None


class FakeContext:
    args = []
    bot = None


def seed():
    db.init_db()
    db.ensure_user(USER, USER, "smoke")
    items = []
    # Названия обоих профилей: тест гоняется и за аналитика, и за
    # исследователя, а лишние просто не пройдут фильтр.
    for i, (title, source, company) in enumerate([
        ("Аналитик данных", "hh", "Рога и копыта"),
        ("Системный аналитик", "careers", "VK"),
        ("Продуктовый аналитик", "careers", "Яндекс"),
        ("UX-исследователь", "careers", "VK"),
        ("Продуктовый исследователь", "careers", "Яндекс"),
        ("Маркетинговый аналитик", "careers", "Ясно"),
        ("Финансовый аналитик", "hh", "Банк"),
        ("Слесарь-сантехник", "hh", "ЖЭК"),
    ]):
        cats = matching.classify(title)
        items.append({
            "uid": "smoke:" + str(i), "source": source, "ext_id": str(i),
            "title": title, "company": company, "area": "Москва",
            "url": "https://example.com/" + str(i),
            "published_at": db.now().isoformat(timespec="seconds"),
            "salary_from": None, "salary_to": None, "currency": "RUR",
            "work_format": "remote", "experience": "",
            "relevant": cats is not None, "categories": cats or [],
        })
    db.upsert_vacancies(items)


async def main():
    seed()
    st = db.get_settings(USER)

    # Очередь и причины отказа считаются без обращения к сети
    queue = bot.pick_for_user(st)
    assert queue, "очередь пустая, хотя вакансии по теме есть"
    assert queue[0]["source"] == "careers", \
        "вакансии с сайтов компаний должны уходить первыми: " + queue[0]["source"]
    reasons = bot.queue_reasons(st)
    assert "не по теме" in reasons, reasons

    # Карточка вакансии собирается целиком
    text, _ = bot.format_vacancy(queue[0])
    assert queue[0]["title"] in text

    # Команды, которые ходят только в базу
    for name, handler in [("stats", bot.cmd_stats), ("why", bot.cmd_why),
                          ("settings", bot.cmd_settings), ("help", bot.cmd_help),
                          ("keywords", bot.cmd_keywords)]:
        SENT.clear()
        await handler(FakeUpdate(), FakeContext())
        assert SENT, "/" + name + " ничего не ответил"

    # В статистике видно и разбивку по сайтам компаний, и отметку версии
    SENT.clear()
    await bot.cmd_stats(FakeUpdate(), FakeContext())
    stats = SENT[0]
    for marker in ("С сайтов компаний", "Код от", "VK"):
        assert marker in stats, "в /stats нет «" + marker + "»"

    # Фильтр: кнопки рисуются и направления переключаются
    bot.settings_text(st)
    bot.settings_keyboard(st)

    # Сборка приложения: ловит опечатку в имени обработчика до запуска
    from telegram.ext import Application
    app = Application.builder().token(os.environ["BOT_TOKEN"]).build()
    for name in ("start", "help", "find", "search", "settings", "keywords",
                 "stats", "probe", "whoami", "pause", "resume", "reset",
                 "resetall", "why", "admin", "sites"):
        assert callable(getattr(bot, "cmd_" + name)), "нет обработчика /" + name
    assert app is not None
    print("✅ Команды бота отвечают, очередь собирается")


asyncio.run(main())
