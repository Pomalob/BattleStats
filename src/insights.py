"""Facts about a period of battles, for the language model that puts them into words.

Python decides what is true and how sure we are; the model only phrases it. Every
comparison carries the number of battles behind it and a confidence level from a plain
z-test, so the model is told in the data itself which differences are noise.

Input: the rows of database.get_period_rows (one per player per battle, the recorder's side,
`focus` marking "our" players). Output: a small JSON-ready dict, schema `period.v1`
(prompt_for() below gives the instructions the model gets with it).

Players are anonymised: the viewer's own accounts are "ты" (the most played one) and
"ты-2", "ты-3"..., everyone else P1, P2... by battles played. One person's accounts never
meet in a battle, so they are never teammates of each other: the facts say so explicitly.
No nicknames, account ids or clan tags leave this module.
"""
import math
import re
from collections import defaultdict
from pathlib import Path
from statistics import fmean, pvariance

import wn8

SCHEMA = "period.v1"

MODE_LABEL = {
    "standard": "Стандартный бой (случайные 15×15)",
    "clan": "Клановые бои",
    "onslaught": "«Натиск» (ранговые 7×7)",
    "other": "Другие режимы",
}
WHO_LABEL = {
    "standard": "только ты и твой взвод; остальные союзники — случайные игроки",
    "onslaught": "только ты и твой взвод; остальные союзники — случайные игроки",
    "clan": "вся команда клана",
    "other": "вся сторона",
}

MIN_PLAYER_BATTLES = 5   # a player with fewer battles gets no facts of their own
MIN_GROUP_BATTLES = 5    # the same for a map, a tank, a half of the period
MAX_PLAYERS = 8          # facts for the most active players only, to keep the input small
MAX_TANKS = 2            # tank facts per player
FORM_LAST = 10           # "recent form": the last N battles against the ones before

# Confidence levels, as the model sees them.
SURE, LIKELY, NOISE, INFO = "надёжно", "вероятно", "шум", "справочно"

# No tier in the replay or the norms table: the norm's damage stands in for it. Rough bands,
# good enough to tell a tier X from a tier V; the facts call them approximate.
LEVELS = ((1500, "высокий (примерно VIII–X)"), (1000, "средний (примерно VII–VIII)"),
          (0, "низкий (примерно VI и ниже)"))
LOW_LEVEL = LEVELS[-1][1]

UNIT_NORM = "% выше (+) или ниже (−) нормы"
UNIT_TEAM = "% выше (+) или ниже (−) союзников в тех же боях"
UNIT_SAME_TANK = "% выше (+) или ниже (−) остальных на этом танке"


# --- statistics ---

def confidence(z: float, n: int) -> str:
    """How far a difference is from chance: |z| 1.96 is p≈0.05, 2.58 is p≈0.01."""
    if n < 10 or abs(z) < 1.96:
        return NOISE
    return SURE if abs(z) >= 2.58 else LIKELY


def z_prop(k: int, n: int, p0: float) -> float:
    """k successes in n against an expected rate p0."""
    if n == 0 or not 0 < p0 < 1:
        return 0.0
    return (k - n * p0) / math.sqrt(n * p0 * (1 - p0))


def z_two_props(k1: int, n1: int, k2: int, n2: int) -> float:
    if not n1 or not n2:
        return 0.0
    p = (k1 + k2) / (n1 + n2)
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    return (k1 / n1 - k2 / n2) / se if se else 0.0


def z_mean(xs: list[float], mu: float) -> float:
    if len(xs) < 3:
        return 0.0
    se = math.sqrt(pvariance(xs) / len(xs))
    return (fmean(xs) - mu) / se if se else 0.0


def z_two_means(a: list[float], b: list[float]) -> float:
    if len(a) < 3 or len(b) < 3:
        return 0.0
    se = math.sqrt(pvariance(a) / len(a) + pvariance(b) / len(b))
    return (fmean(a) - fmean(b)) / se if se else 0.0


def pct(part: float, whole: float) -> float | None:
    return round(part * 100 / whole, 1) if whole else None


def direction(diff: float) -> str:
    return "выше" if diff > 0 else "ниже" if diff < 0 else "так же"


# --- facts ---

class _Facts:
    def __init__(self):
        self.items: list[dict] = []

    def add(self, about: str, metric: str, label: str, **fields) -> None:
        self.items.append({"id": f"f{len(self.items) + 1}", "about": about, "metric": metric,
                           "label": label, **fields})


def _norm(row: dict) -> dict | None:
    try:
        return wn8.expected().get(int(row["tank_id"])) if row.get("tank_id") else None
    except ValueError:
        return None


def _vs_norm(rows: list[dict], key: str, exp_key: str):
    """Sum of `key` against the sum of its norm, plus the per-battle ratios for the z-test."""
    known = [(r, _norm(r)) for r in rows]
    known = [(r, e) for r, e in known if e and e[exp_key] > 0 and r.get(key) is not None]
    if not known:
        return None
    actual = sum(float(r[key]) for r, _ in known)
    expected = sum(e[exp_key] for _, e in known)
    ratios = [float(r[key]) / e[exp_key] for r, e in known]
    return {
        "n": len(known),
        "diff_pct": round((actual / expected - 1) * 100),
        "actual_avg": round(actual / len(known), 1),
        "norm_avg": round(expected / len(known), 1),
        "z": z_mean(ratios, 1.0),
        "ratios": ratios,
    }


def _against(v: dict, base: dict | None, comparable: bool) -> dict:
    """The fields of a "vs norm" fact. Where the norms do not fit the mode, the player is
    compared with the rest of the team instead: their ratio to the norm against the team's."""
    if comparable or not base:
        return {"value": v["diff_pct"], "unit": UNIT_NORM, "actual": v["actual_avg"],
                "norm": v["norm_avg"], "n": v["n"], "z": v["z"]}
    rel = (1 + v["diff_pct"] / 100) / (1 + base["diff_pct"] / 100) - 1
    return {"value": round(rel * 100), "unit": UNIT_TEAM, "actual": v["actual_avg"],
            "n": v["n"], "z": z_two_means(v["ratios"], base["ratios"])}


def level_of(row: dict) -> str | None:
    """Approximate tier band of the tank in this row, from its damage norm."""
    exp = _norm(row)
    if not exp:
        return None
    return next(label for floor, label in LEVELS if exp["expDamage"] >= floor)


def _levels(rows: list[dict]) -> dict[str, int]:
    """Share of battles in each level band, %."""
    known = [lv for lv in map(level_of, rows) if lv]
    return {lv: round(known.count(lv) * 100 / len(known)) for _, lv in LEVELS if lv in known}


def _wn8(rows: list[dict]) -> int | None:
    tally = wn8.Tally()
    for r in rows:
        tally.add(r.get("tank_id") if (r.get("tank_id") or "").isdigit() else None, 1,
                  r["damage_dealt"], r["frags"], r.get("spotted") or 0, 0, r["result"] == "win")
    return tally.score()


def _early_death(r: dict) -> bool:
    """Killed within the first third of the battle."""
    return (not r["survived"] and r.get("lifetime") is not None and r.get("duration")
            and float(r["lifetime"]) < float(r["duration"]) / 3)


def _expected_winrate(rows: list[dict]) -> float | None:
    rates = [e["expWinRate"] / 100 for e in map(_norm, rows) if e]
    return fmean(rates) if rates else None


def _rating_facts(facts: "_Facts", rating_rows: list[dict], labels: dict[str, str]) -> None:
    """Onslaught rating of the viewer's own accounts: only the recorder's is in a replay."""
    by_name: dict[str, list[dict]] = defaultdict(list)
    for r in rating_rows:
        if r["name"] in labels and not r["qualifying"] and r["rating"]:
            by_name[r["name"]].append(r)
    for name, rs in by_name.items():
        if len(rs) < MIN_GROUP_BATTLES:
            continue
        who = labels[name]
        deltas = [r["delta"] or 0 for r in rs]
        # The replay holds the rating *before* the battle and the battle's change. Where the
        # next battle does not start from the last one's result, battles were played without
        # a saved replay: the real change and the change over the recorded battles differ.
        after = [r["rating"] + d for r, d in zip(rs, deltas)]
        start, end = rs[0]["rating"], after[-1]
        gaps = sum(1 for prev, r in zip(after, rs[1:]) if r["rating"] != prev)
        facts.add(who, "rating_change", "Рейтинг «Натиска» за период: value — изменение от "
                  "рейтинга перед первым боем (before) до рейтинга после последнего (actual); "
                  "recorded — сумма изменений только по боям с реплеями; unrecorded — сколько "
                  "рейтинга дали или отняли бои без реплея; peak — наибольший рейтинг; gaps — "
                  "сколько раз между соседними реплеями были бои без реплея",
                  value=end - start, unit="очков рейтинга", before=start, actual=end,
                  recorded=sum(deltas), unrecorded=end - start - sum(deltas), peak=max(after),
                  gaps=gaps, n=len(rs),
                  direction=direction(end - start),
                  confidence=confidence(z_mean([float(d) for d in deltas], 0.0), len(rs)))
        won = [d for r, d in zip(rs, deltas) if r["result"] == "win"]
        lost = [d for r, d in zip(rs, deltas) if r["result"] != "win"]
        if won and lost:
            facts.add(who, "rating_per_result", "Рейтинг «Натиска»: в среднем за победу (value) и "
                      "за поражение или ничью (before)", value=round(fmean(won)),
                      before=round(fmean(lost)), unit="очков рейтинга за бой", n=len(rs),
                      n_wins=len(won), n_losses=len(lost), confidence=INFO)
        if len(rs) >= 2 * MIN_GROUP_BATTLES:
            half = len(deltas) // 2
            early, late = [float(d) for d in deltas[:half]], [float(d) for d in deltas[half:]]
            facts.add(who, "rating_trend", "Рейтинг «Натиска»: среднее изменение за бой во второй "
                      "половине боёв периода против первой", value=round(fmean(late), 1),
                      before=round(fmean(early), 1), unit="очков рейтинга за бой", n=len(rs),
                      direction=direction(fmean(late) - fmean(early)),
                      confidence=confidence(z_two_means(late, early), half))


_NAME_CHARS = re.compile(r"[^\w\s.,'()«»№/+-]", re.UNICODE)


def clean_name(value) -> str:
    """A tank or map name as it may go into the facts. Both come from the replay file, which the
    uploader controls: cap the length and drop anything but name-like characters, so a crafted
    replay cannot smuggle instructions into the model's input."""
    text = _NAME_CHARS.sub("", str(value or ""))
    return " ".join(text.split())[:40] or "—"


def build_period_facts(rows: list[dict], me, mode: str | None,
                       date_from: str | None = None, date_to: str | None = None,
                       rating_rows: list[dict] | None = None) -> dict:
    rows = [{**r, "vehicle": clean_name(r.get("vehicle")), "map_name": clean_name(r.get("map_name"))}
            for r in rows]
    focus = [r for r in rows if r["focus"]]
    battles = {}
    for r in focus:
        battles.setdefault(r["battle_id"], r)
    order = list(battles)  # rows come oldest first
    mode = mode or "mixed"

    # --- who is who, without names ---
    # `me`: the in-game names the viewer records under (one name, or several accounts).
    mine = {me} if isinstance(me, str) else set(me or ())
    per_player: dict[str, list[dict]] = defaultdict(list)
    for r in focus:
        per_player[r["name"]].append(r)
    ranked = sorted(per_player, key=lambda name: (name not in mine, -len(per_player[name]), name))
    labels, own, others = {}, 0, 0
    for name in ranked:
        if name in mine:
            own += 1
            labels[name] = "ты" if own == 1 else f"ты-{own}"
        else:
            others += 1
            labels[name] = f"P{others}"

    facts = _Facts()
    norms_comparable = mode == "standard"

    # --- the team as a whole: one result per battle ---
    results = [battles[b]["result"] for b in order]
    n = len(results)
    wins = results.count("win")
    # Onslaught matchmaking pairs players of a similar rating, so the honest baseline is 50%.
    if mode == "onslaught":
        exp_rate, norm_label = 0.5, ("norm — 50%: подбор в «Натиске» сводит игроков близкого "
                                     "рейтинга, равные шансы")
    else:
        exp_rate, norm_label = _expected_winrate(focus), "norm — средний процент побед на этих танках по серверу"
    if n:
        fields = {"value": pct(wins, n), "unit": "%", "n": n,
                  "wins": wins, "losses": results.count("loss"), "draws": results.count("draw")}
        if exp_rate:
            z = z_prop(wins, n, exp_rate)
            fields.update(norm=round(exp_rate * 100, 1), direction=direction(wins / n - exp_rate),
                          confidence=confidence(z, n))
        facts.add("команда", "winrate", f"Процент побед за период; {norm_label}", **fields)
    if mode == "onslaught" and rating_rows:
        _rating_facts(facts, rating_rows, {n_: labels[n_] for n_ in labels if n_ in mine})

    # Trend: the later half of the period's battles against the earlier half.
    if n >= 2 * MIN_GROUP_BATTLES:
        half = n // 2
        early, late = results[:half], results[half:]
        k1, k2 = early.count("win"), late.count("win")
        z = z_two_props(k2, len(late), k1, len(early))
        facts.add("команда", "trend_winrate", "Процент побед: вторая половина боёв периода "
                  "против первой", value=pct(k2, len(late)), before=pct(k1, len(early)), unit="%",
                  n=n, direction=direction(k2 / len(late) - k1 / len(early)),
                  confidence=confidence(z, min(len(early), len(late))))

    # Maps: only the ones that differ from the period's own win rate beyond chance.
    by_map: dict[str, list[str]] = defaultdict(list)
    for b in order:
        by_map[battles[b]["map_name"]].append(battles[b]["result"])
    quiet_maps = 0
    for name, res in sorted(by_map.items(), key=lambda kv: -len(kv[1])):
        if len(res) < MIN_GROUP_BATTLES or not n:
            continue
        k = res.count("win")
        z = z_prop(k, len(res), wins / n) if 0 < wins < n else 0.0
        conf = confidence(z, len(res))
        if conf == NOISE:
            quiet_maps += 1
            continue
        facts.add(name, "map_winrate", "Процент побед на карте против процента побед за весь "
                  "период", value=pct(k, len(res)), norm=pct(wins, n), unit="%", n=len(res),
                  direction=direction(k / len(res) - wins / n), confidence=conf)
    if quiet_maps:
        facts.add("карты", "maps_without_difference", "Сколько карт сыграно от 5 раз, но их "
                  "процент побед не отличается от среднего сильнее случайности",
                  value=quiet_maps, unit="карт", confidence=NOISE)

    # --- players ---
    allies_by_battle: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        allies_by_battle[r["battle_id"]].append(r)

    players_out = []
    for name in ranked:
        prow = per_player[name]
        entry = {"id": labels[name], "battles": len(prow), "wn8": _wn8(prow),
                 "levels": _levels(prow)}
        if name in mine:
            entry["own_account"] = True
        players_out.append(entry)
        if len(prow) < MIN_PLAYER_BATTLES or len(players_out) > MAX_PLAYERS:
            continue
        who = labels[name]
        always = name in mine

        facts.add(who, "wn8", "WN8 за период", value=entry["wn8"], n=len(prow),
                  confidence=INFO)

        # The baseline wherever norms do not fit: the allies in the very same battles (in clan
        # battles the team itself, in Onslaught mostly random allies).
        mates = [a for r in prow for a in allies_by_battle[r["battle_id"]] if a["name"] != name]
        for key, exp_key, metric, label in (
            ("damage_dealt", "expDamage", "damage_vs_norm", "Средний урон"),
            ("frags", "expFrag", "frags_vs_norm", "Фраги за бой"),
            ("spotted", "expSpot", "spotting_vs_norm", "Засвеченные противники за бой"),
        ):
            v = _vs_norm(prow, key, exp_key)
            if not v:
                continue
            f = _against(v, _vs_norm(mates, key, exp_key), norms_comparable)
            conf = confidence(f.pop("z"), f["n"])
            if conf == NOISE and not (always or metric == "damage_vs_norm"):
                continue
            label += (" против нормы его танков" if f["unit"] == UNIT_NORM else
                      " против союзников в тех же боях: у каждого считается отношение к норме своих "
                      "танков, и эти отношения сравниваются (это не сравнение на одних и тех же "
                      "танках)")
            facts.add(who, metric, label, **f, direction=direction(f["value"]), confidence=conf)

        # Onslaught's own score: prestige points against the allies in the same battles.
        if mode == "onslaught":
            own_pp = [float(r["prestige"]) for r in prow if r.get("prestige") is not None]
            mates_pp = [float(r["prestige"]) for r in mates if r.get("prestige") is not None]
            if len(own_pp) >= MIN_PLAYER_BATTLES and len(mates_pp) >= MIN_PLAYER_BATTLES and fmean(mates_pp):
                conf = confidence(z_two_means(own_pp, mates_pp), len(own_pp))
                facts.add(who, "prestige", "Очки престижа за бой против союзников в тех же боях",
                          value=round((fmean(own_pp) / fmean(mates_pp) - 1) * 100), unit=UNIT_TEAM,
                          actual=round(fmean(own_pp), 1), norm=round(fmean(mates_pp), 1),
                          n=len(own_pp), direction=direction(fmean(own_pp) - fmean(mates_pp)),
                          confidence=conf)

        # Survival and early deaths against the allies in the very same battles.
        for metric, label, test in (
            ("survival", "Доля боёв, которые пережил, против союзников в тех же боях",
             lambda r: bool(r["survived"])),
            ("early_deaths", "Доля боёв, где погиб в первой трети боя, против союзников в тех же боях",
             _early_death),
        ):
            k1, n1 = sum(map(test, prow)), len(prow)
            k2, n2 = sum(map(test, mates)), len(mates)
            if not n2:
                continue
            z = z_two_props(k1, n1, k2, n2)
            conf = confidence(z, n1)
            if conf == NOISE and not always:
                continue
            facts.add(who, metric, label, value=pct(k1, n1), norm=pct(k2, n2), unit="%", n=n1,
                      direction=direction(k1 / n1 - k2 / n2), confidence=conf)

        # Recent form: damage against the norm, the last battles against the ones before.
        dmg = _vs_norm(prow, "damage_dealt", "expDamage")
        if dmg and dmg["n"] >= FORM_LAST + MIN_GROUP_BATTLES * 2:
            recent, before = dmg["ratios"][-FORM_LAST:], dmg["ratios"][:-FORM_LAST]
            z = z_two_means(recent, before)
            conf = confidence(z, FORM_LAST)
            if conf != NOISE or always:
                facts.add(who, "recent_form", f"Урон против нормы: последние {FORM_LAST} боёв "
                          "против предыдущих", value=round((fmean(recent) - 1) * 100),
                          before=round((fmean(before) - 1) * 100), unit=UNIT_NORM, n=len(prow),
                          direction=direction(fmean(recent) - fmean(before)), confidence=conf)

        # Platoon or solo: only in matchmade modes, where it is a choice.
        if mode in ("standard", "onslaught"):
            squad = [r for r in prow if r["platoon"] != "0"]
            solo = [r for r in prow if r["platoon"] == "0"]
            if len(squad) >= MIN_GROUP_BATTLES and len(solo) >= MIN_GROUP_BATTLES:
                ks = sum(r["result"] == "win" for r in squad)
                ko = sum(r["result"] == "win" for r in solo)
                z = z_two_props(ks, len(squad), ko, len(solo))
                facts.add(who, "platoon_vs_solo", "Процент побед во взводе против боёв в одиночку",
                          value=pct(ks, len(squad)), before=pct(ko, len(solo)), unit="%",
                          n=len(prow), n_platoon=len(squad), n_solo=len(solo),
                          direction=direction(ks / len(squad) - ko / len(solo)),
                          confidence=confidence(z, min(len(squad), len(solo))))

        # Tanks: the most played ones, and any that clearly stand out.
        by_tank: dict[str, list[dict]] = defaultdict(list)
        for r in prow:
            by_tank[r["vehicle"]].append(r)
        tanks = sorted(by_tank.items(), key=lambda kv: -len(kv[1]))
        shown = 0
        for tank, trows in tanks:
            if len(trows) < MIN_GROUP_BATTLES or len(tanks) < 2 or shown >= MAX_TANKS:
                continue
            if norms_comparable:
                v = _vs_norm(trows, "damage_dealt", "expDamage")
                if not v:
                    continue
                f = _against(v, None, True)
                label = f"Танк {tank}: урон на нём против нормы этого танка"
            else:
                # A tank can be strong in this mode for everyone: compare with the allies on the
                # very same tank anywhere in the period, never with other tanks. In Onslaught
                # the measure is the mode's own score, prestige points.
                key, what = (("prestige", "очки престижа") if mode == "onslaught"
                             else ("damage_dealt", "средний урон"))
                same = [float(r[key]) for r in rows
                        if r["name"] != name and r["vehicle"] == tank and r.get(key) is not None]
                own_v = [float(r[key]) for r in trows if r.get(key) is not None]
                if len(same) < MIN_GROUP_BATTLES or len(own_v) < MIN_GROUP_BATTLES or not fmean(same):
                    continue
                f = {"value": round((fmean(own_v) / fmean(same) - 1) * 100), "unit": UNIT_SAME_TANK,
                     "actual": round(fmean(own_v), 1), "norm": round(fmean(same), 1),
                     "n": len(own_v), "n_others": len(same), "z": z_two_means(own_v, same)}
                label = f"Танк {tank}: {what} на нём против других союзников на этом же танке"
            conf = confidence(f.pop("z"), f["n"])
            if conf == NOISE:
                continue
            shown += 1
            facts.add(who, "tank", label, tank=tank, **f,
                      share=round(len(trows) * 100 / len(prow)), level=level_of(trows[0]),
                      wn8=_wn8(trows), direction=direction(f["value"]), confidence=conf)

    return {
        "schema": SCHEMA,
        "mode": MODE_LABEL.get(mode, "Все режимы"),
        "period": {"from": date_from or (battles[order[0]]["date_time"][:10] if order else None),
                   "to": date_to or (battles[order[-1]]["date_time"][:10] if order else None)},
        "battles": n,
        "who": WHO_LABEL.get(mode, "все игроки выбранных боёв"),
        "norms": {
            "source": "XVM, средние значения по танку на серверах Лесты",
            "comparable": norms_comparable,
            "note": ("Нормы посчитаны по случайным боям: в этом режиме сравнение с ними честное."
                     if norms_comparable else
                     "Нормы посчитаны по случайным боям: в этом режиме все игроки выше или ниже "
                     "нормы из-за самого режима. Сравнивай игроков между собой, а не с нормой."),
        },
        "players": players_out,
        "levels_note": ("levels у игрока и level у танка — приблизительный уровень техники, "
                        "выведенный из нормы урона танка. На уровне «" + LOW_LEVEL + "» сильный "
                        "игрок почти всегда намного выше нормы, поэтому сравнение с нормой там "
                        "мало говорит о навыке."),
        "accounts": (
            f"Бои записаны с {own} твоих аккаунтов: "
            + ", ".join(labels[n] for n in ranked if n in mine)
            + ". Это один человек: такие аккаунты никогда не играют в одном бою и не бывают "
              "друг другу напарниками. Сравнивать их можно только как два разных набора твоих боёв."
            if own > 1 else None),
        "facts": facts.items,
        "not_in_data": [
            "позиции и действия в бою: где стоял, куда ехал, в кого стрелял",
            "состав и уровень противников",
            "оборудование, снаряды и экипаж",
            "что было до и после боя: стратегия, звонки, договорённости",
        ],
    }


# --- the instructions for the model ---

PROMPTS = Path(__file__).with_name("prompts")
PROMPT_VERSION = "v1"


def prompt_for(mode: str | None) -> str:
    """The system prompt for a period in this mode: the shared rules plus the mode's own part
    (prompts/base_v1.md + prompts/<mode>_v1.md). Modes without a part get the rules alone."""
    text = (PROMPTS / f"base_{PROMPT_VERSION}.md").read_text(encoding="utf-8")
    part = PROMPTS / f"{mode}_{PROMPT_VERSION}.md"
    if mode and part.is_file():
        text += "\n" + part.read_text(encoding="utf-8")
    return text
