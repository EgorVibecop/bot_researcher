"""
Проверки склейки вакансий, фильтра по свежести и оформления карточки.
Сеть не используется. Запуск:  python test_merge.py
"""

import sys
from datetime import datetime, timedelta, timezone

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import search  # noqa: E402

now = datetime.now(timezone.utc)
failures = []


def check(name, got, expected):
    if got != expected:
        failures.append(f"{name}\n    ожидалось: {expected!r}\n    получено:  {got!r}")


def vac(title, source="hh", url=None, company="Ромашка", days_ago=1, **kw):
    item = {
        "uid": f"{source}:{abs(hash((title, source, url))) % 99999}",
        "source": source,
        "title": title,
        "company": company,
        "url": url or f"https://{source}.example/{abs(hash(title)) % 999}",
        "published_at": (now - timedelta(days=days_ago)).isoformat(),
        "work_format": "",
        "salary_from": None,
        "salary_to": None,
        "currency": "RUR",
        "area": "",
        "experience": "",
    }
    item.update(kw)
    return item


# --- ключ склейки ---
check(
    "грейд и скобки не мешают опознать вакансию",
    search.merge_key(vac("Senior UX-исследователь (удалённо)")),
    search.merge_key(vac("UX-исследователь")),
)
check(
    "организационная приставка в компании игнорируется",
    search.merge_key(vac("Аналитик", company="ООО Ромашка")),
    search.merge_key(vac("Аналитик", company="Ромашка")),
)

# --- склейка между источниками ---
merged = search.merge_duplicates([
    vac("UX-исследователь", source="hh", url="https://hh.ru/vacancy/1"),
    vac("Senior UX-исследователь (удалённо)", source="habr",
        url="https://career.habr.com/vacancies/2", work_format="remote"),
    vac("UX исследователь", source="getmatch", url="https://getmatch.ru/3",
        salary_from=250000, salary_to=350000),
])
check("одна вакансия из трёх сервисов", len(merged), 1)
check("собраны ссылки на все три", len(merged[0]["links"]), 3)
check("источники перечислены", [s for s, _ in merged[0]["links"]],
      ["hh", "habr", "getmatch"])
check("зарплата подтянулась оттуда, где была", merged[0]["salary_from"], 250000)
check("формат работы подтянулся", merged[0]["work_format"], "remote")

# разные компании — разные вакансии
two = search.merge_duplicates([
    vac("Аналитик", company="Ромашка", url="https://a/1"),
    vac("Аналитик", company="Одуванчик", source="habr", url="https://b/2"),
])
check("одинаковое название у разных компаний не склеивается", len(two), 2)

# --- свежесть ---
check("свежая проходит", search.is_fresh(vac("A", days_ago=10)), True)
check("старше трёх месяцев отсекается", search.is_fresh(vac("A", days_ago=120)), False)
check("на границе проходит", search.is_fresh(vac("A", days_ago=91)), True)
check("архивная не проходит",
      search.is_fresh(vac("A", days_ago=2, archived=True)), False)
check("без даты не теряется",
      search.is_fresh(dict(vac("A"), published_at=None)), True)

# --- карточка ---
import bot  # noqa: E402

text, keyboard = bot.format_vacancy(merged[0])
if "не указана" in text:
    failures.append("зарплата известна, а карточка пишет «не указана»:\n" + text)

labels = [b.text for row in keyboard.inline_keyboard for b in row]
for expected_label in ("hh.ru", "Хабр Карьера", "getmatch"):
    if not any(expected_label in x for x in labels):
        failures.append(f"нет кнопки {expected_label!r}, есть: {labels}")

no_salary, _ = bot.format_vacancy(vac("Без зарплаты"))
if "💰 не указана" not in no_salary:
    failures.append("без зарплаты должно писать «не указана»:\n" + no_salary)

# в названии могут быть угловые скобки — разметка не должна ломаться
danger, _ = bot.format_vacancy(vac("C++ <b>dev</b> & Co"))
if "<b>dev</b>" in danger.replace("<b>C++", ""):
    failures.append("HTML в названии не экранирован:\n" + danger)

print()
if failures:
    print(f"❌ Провалено проверок: {len(failures)}\n")
    for f in failures:
        print(" -", f)
    raise SystemExit(1)
print("✅ Все проверки пройдены")
