"""Check a language model's answer against the facts it was given (insights.prompt_for).

Catches the failures that matter: a claim with no fact behind it, a number that is not in the
facts the point cites, a conclusion drawn from noise or with no number in it, hedging on a
difference that is not noise, a misread comparison, the viewer's own alts called teammates.
Returns a list of problems; an empty list means the answer passed. Problems that start with
"предупреждение" are worth a look but do not make the answer wrong.
"""
import json
import re

from insights import LIKELY, SURE, UNIT_SAME_TANK, UNIT_TEAM

KINDS = {"сильная сторона", "подтянуть", "наблюдение"}
CONCLUSIONS = {"сильная сторона", "подтянуть"}
MAX_STRENGTHS = 3
FEW_BATTLES = 20  # below this, "few battles" caveats are expected, not hedging

_NUMBER = re.compile(r"\d+(?:[.,]\d+)?")
# "Too early to judge" and the like. Not a bare "рано": "редко погибаешь рано" is a fact.
_HEDGE = re.compile(r"рано\s+(?:судить|делать|говорить|считать)|тенденц\w*\s+рано|пока не тенденц"
                    r"|предварительн|нельзя\s+(?:делать|судить)|в пределах случайн", re.I)
_SAME_TANKS = re.compile(r"(?:на|в) (?:тех|одних) же (?:и тех же )?танк", re.I)
_ALT_AS_MATE = re.compile(r"(?:вместе|взвод\w*|напарник\w*)\s+с\s+ты-\d", re.I)
# Advice the facts cannot back: roles, tank choice, tactics, positions.
_UNBACKED_ADVICE = re.compile(r"\bрол[ьиейя]\w*|тактик|позици|выб(?:ор|рать|ирать)\w* танк", re.I)


def _without_names(text: str, facts: list[dict]) -> str:
    """The text with tank and map names cut out: "RDT 62" is not a number the point states."""
    for f in facts:
        for name in (f.get("tank"), f.get("about")):
            if isinstance(name, str) and any(ch.isdigit() for ch in name):
                text = text.replace(name, " ")
    return text


def _numbers(value) -> set[float]:
    """Every number in a fact: its numeric fields and the digits in its strings (tank names)."""
    out: set[float] = set()
    if isinstance(value, bool):
        return out
    if isinstance(value, (int, float)):
        out.add(abs(float(value)))
    elif isinstance(value, str):
        out.update(float(m.replace(",", ".")) for m in _NUMBER.findall(value))
    elif isinstance(value, dict):
        for v in value.values():
            out |= _numbers(v)
    elif isinstance(value, list):
        for v in value:
            out |= _numbers(v)
    return out


def _allowed(number: float, pool: set[float]) -> bool:
    # Rounding to a whole number is allowed: 72.9 may be said as 73.
    return any(abs(number - p) < 1e-6 or round(p) == number for p in pool)


def _text_numbers(text: str) -> list[float]:
    clean = text.replace(" ", "").replace(" ", "")
    return [float(m.replace(",", ".")) for m in _NUMBER.findall(clean)]


def check(answer, facts: dict) -> list[str]:
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except json.JSONDecodeError as e:
            return [f"ответ не JSON: {e.msg}"]
    if not isinstance(answer, dict):
        return ["ответ должен быть объектом"]

    by_id = {f["id"]: f for f in facts.get("facts", [])}
    own = {p["id"] for p in facts.get("players", []) if p.get("own_account")} or {"ты"}
    # Numbers anyone may use anywhere: the period's size, the players' battles and WN8.
    common = (_numbers(facts.get("battles")) | _numbers(facts.get("players"))
              | _numbers(facts.get("period")))
    problems = []

    points = answer.get("points")
    if not isinstance(points, list) or not 3 <= len(points) <= 6:
        problems.append("points: нужно от 3 до 6 пунктов")
        points = points if isinstance(points, list) else []
    strengths = sum(1 for p in points if p.get("kind") == "сильная сторона")
    if strengths > MAX_STRENGTHS:
        problems.append(f"points: «сильных сторон» {strengths}, можно не больше {MAX_STRENGTHS}")

    covered: set[str] = set()
    for i, p in enumerate(points, 1):
        where = f"пункт {i}"
        text = p.get("text", "")
        kind = p.get("kind")
        if kind not in KINDS:
            problems.append(f"{where}: неизвестный kind {kind!r}")
        cited = p.get("facts") or []
        if not cited:
            problems.append(f"{where}: нет ссылок на факты")
            continue
        missing = [c for c in cited if c not in by_id]
        if missing:
            problems.append(f"{where}: ссылки на несуществующие факты {missing}")
        known = [by_id[c] for c in cited if c in by_id]
        covered |= {f["about"] for f in known}
        sure = [f for f in known if f.get("confidence") in (SURE, LIKELY)]

        if kind in CONCLUSIONS and known and not sure:
            problems.append(f"{where}: вывод «{kind}» только из фактов без уверенности")
        numbers = _text_numbers(text)
        if kind in CONCLUSIONS and not _text_numbers(_without_names(text, list(by_id.values()))):
            problems.append(f"{where}: вывод «{kind}» без единой цифры")
        if _UNBACKED_ADVICE.search(text):
            problems.append(f"предупреждение: {where}: совет про роли, танки или тактику — "
                            "в фактах этого нет")
        pool = common | _numbers(known)
        for number in numbers:
            if not _allowed(number, pool):
                problems.append(f"{where}: числа {number:g} нет в процитированных фактах")
        if (known and len(sure) == len(known) and all((f.get("n") or 0) >= FEW_BATTLES for f in known)
                and _HEDGE.search(text)):
            problems.append(f"{where}: оговорка про случайность, хотя все факты «{SURE}» или "
                            f"«{LIKELY}» и боёв достаточно")
        # Only a team comparison through the norms can be misread this way: the win rate's
        # norm really is "on the same tanks".
        if (_SAME_TANKS.search(text) and any(f.get("unit") == UNIT_TEAM for f in known)
                and not any(f.get("unit") == UNIT_SAME_TANK for f in known)):
            problems.append(f"{where}: «на тех же танках», но сравнение было не на одном танке")
        if _ALT_AS_MATE.search(text):
            problems.append(f"{where}: свой второй аккаунт назван напарником")

    for field in ("summary", "limits"):
        text = answer.get(field) or ""
        everything = common | _numbers(list(by_id.values()))
        for number in _text_numbers(text):
            if not _allowed(number, everything):
                problems.append(f"{field}: числа {number:g} нет во входных данных")
        units = {f.get("unit") for f in by_id.values()}
        if _SAME_TANKS.search(text) and UNIT_TEAM in units and UNIT_SAME_TANK not in units:
            problems.append(f"{field}: «на тех же танках», но таких сравнений в фактах нет")
        if _ALT_AS_MATE.search(text):
            problems.append(f"{field}: свой второй аккаунт назван напарником")
    if not answer.get("summary"):
        problems.append("summary: пусто")

    others = {f["about"] for f in by_id.values()
              if f.get("confidence") in (SURE, LIKELY) and f["about"] not in own}
    if others and not covered & others:
        problems.append("предупреждение: есть значимые факты о команде или других игроках, "
                        "но ни один пункт о них не говорит")
    return problems
