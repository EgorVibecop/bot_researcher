"""Проверки на молчаливую потерю вакансий по дороге к человеку.

Каждый тест воспроизводит сценарий, в котором вакансия раньше исчезала.
"""
import os
import tempfile

os.environ.setdefault("PROFILE", "analyst")
DB_DIR = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(DB_DIR, "losses.db")

import db  # noqa: E402
import search  # noqa: E402

db.init_db()


def vac(uid, title="Аналитик данных", source="avito", company="Рога",
        area="Москва", fmt="remote"):
    return {"uid": uid, "source": source, "ext_id": uid.split(":")[-1],
            "title": title, "company": company, "area": area,
            "url": "https://example.com/" + uid.replace(":", "/"),
            "published_at": db.now().isoformat(timespec="seconds"),
            "salary_from": None, "salary_to": None, "currency": "RUR",
            "work_format": fmt, "experience": "", "relevant": True,
            "categories": ["data"]}


def live_uids(source):
    conn = db.get_conn()
    rows = conn.execute(
        "SELECT uid FROM vacancies WHERE source = ? AND IFNULL(closed, 0) = 0",
        (source,)).fetchall()
    conn.close()
    return {r["uid"] for r in rows}


def test_closed_revives():
    """Вакансия вернулась в каталог - метка «снята» должна сниматься.

    Иначе любое ложное закрытие (сайт моргнул, отдал полкаталога) означает
    потерю навсегда: closed = 1 раньше никто не снимал.
    """
    batch = [vac("avito:%s" % i, title="Аналитик данных " + str(i)) for i in range(6)]
    db.upsert_vacancies(batch)
    # в выдаче осталось четыре из шести - правдоподобная убыль, закрываем
    db.close_missing("avito", [v["uid"] for v in batch[:4]])
    assert len(live_uids("avito")) == 4, "закрытие не сработало"
    # следующий обход отдал всё
    db.upsert_vacancies(batch)
    assert len(live_uids("avito")) == 6, "вакансии не вернулись после обхода"


def test_partial_harvest_does_not_close():
    """Полкаталога - не повод закрывать остальное."""
    batch = [vac("2gis:%s" % i, source="2gis", title="BI-аналитик " + str(i))
             for i in range(10)]
    db.upsert_vacancies(batch)
    skipped = db.close_missing("2gis", [batch[0]["uid"]])
    assert skipped == 0, "закрыли каталог по одной уцелевшей карточке"
    assert len(live_uids("2gis")) == 10


def test_region_keeps_vacancies_without_city():
    """У карьерных сайтов города нет - это не повод прятать вакансию.

    Пустой формат работы мы уже читаем как «источник не сообщил»; с городом
    должно быть так же, иначе у человека с фильтром «Москва» пропадают все
    вакансии с сайтов компаний, из телеграма и IT-One.
    """
    st = dict(db.SETTINGS_DEFAULTS, user_id=1, region="msk",
              categories="data", work_formats="remote,hybrid,office")
    no_city = vac("careers:1", source="careers", area="", fmt="")
    no_city["categories"] = "data"
    reason = db.match_reason(no_city, st, [], [], set())
    assert reason != "регион", "вакансия без города отброшена фильтром региона"


def test_hh_alive_is_conservative():
    """При непонятном ответе вакансия считается живой."""
    class Boom:
        async def get(self, *a, **kw):
            raise RuntimeError("сеть упала")

    import asyncio
    alive = asyncio.run(search.is_alive(Boom(), {"uid": "hh:1",
                                                 "url": "https://hh.ru/vacancy/1"}))
    assert alive is True, "сетевая ошибка приняли за закрытую вакансию"


def test_queue_capacity_counts_quiet_hours():
    """Запас очереди считается по реальному числу отправок, не по 24 в сутки.

    Ночью бот молчит (23:00-08:00), в дайджесте отправка одна в сутки.
    Раньше запас выходил втрое больше настоящего, и предупреждение
    о выпадающем хвосте не срабатывало там, где потеря уже шла.
    """
    import bot
    instant = {"max_per_run": 8, "quiet_start": 23, "quiet_end": 8,
               "mode": "instant"}
    digest = {"max_per_run": 5, "quiet_start": 23, "quiet_end": 8,
              "mode": "digest"}
    assert bot.runs_a_day(instant) == 15, "тихие часы не учтены"
    assert bot.runs_a_day(digest) == 1, "дайджест - одна отправка в сутки"
    # 8 x 15 x 21 = 2520: столько очередь успевает разослать за окно
    assert bot.queue_warning(instant, 2000) == ""
    assert "хвост выпадет" in bot.queue_warning(instant, 3000)
    # в дайджесте запас всего 5 x 21 = 105
    assert "хвост выпадет" in bot.queue_warning(digest, 200)


class FakeResp:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("не json")
        return self._payload


class FakeClient:
    """Отвечает заранее заданным ответом и запоминает, куда ходили."""

    def __init__(self, resp):
        self.resp = resp
        self.urls = []

    async def get(self, url, **kw):
        self.urls.append(url)
        if isinstance(self.resp, Exception):
            raise self.resp
        return self.resp


def test_hh_archived_is_trusted():
    """У hh спрашиваем про архив напрямую: снятую не отправляем."""
    import asyncio
    vac_hh = {"uid": "hh:1", "source": "hh", "ext_id": "1",
              "url": "https://hh.ru/vacancy/1"}
    client = FakeClient(FakeResp(payload={"archived": True}))
    assert asyncio.run(search.is_alive(client, vac_hh)) is False
    assert "api.hh.ru" in client.urls[0], "не спросили у API"

    client = FakeClient(FakeResp(payload={"archived": False}))
    assert asyncio.run(search.is_alive(client, vac_hh)) is True


def test_hh_block_falls_back_to_page():
    """Если API отказал, проверяем страницу - и живую не теряем."""
    import asyncio
    vac_hh = {"uid": "hh:2", "source": "hh", "ext_id": "2",
              "url": "https://hh.ru/vacancy/2"}
    client = FakeClient(FakeResp(status=403))
    assert asyncio.run(search.is_alive(client, vac_hh)) is True
    # сначала API, потом страница
    assert len(client.urls) == 2 and client.urls[1].startswith("https://hh.ru/")


for name, func in sorted(globals().items()):
    if name.startswith("test_"):
        func()
        print("✅", name)
print("\n✅ Проверки на потери пройдены")
