"""AI analysis: the endpoint (cache, limit, errors) and the model call (retry, JSON extraction)."""
import json
from types import SimpleNamespace

import pytest

import config
import explainer


def _seed(fake_db, user_id, n=12, mode="clan"):
    for i in range(n):
        fake_db.save_battle({
            "battle_hash": f"{mode}-{i}", "filename": f"{i}.mtreplay", "map_name": "Энск",
            "date_time": f"2026-11-{1 + i % 9:02d} 20:{i:02d}:00", "result": "win" if i % 3 else "loss",
            "mode": mode, "player_team": 1, "winner_team": 1, "recorder": {"playerName": "Me"},
            "players": [{"name": n_, "vehicle": "T-34", "team": 1, "damage_dealt": 1000 + 100 * k}
                        for k, n_ in enumerate(("Me", "Mate"))],
        }, user_id)


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(config, "EXPLAIN_ENABLED", True)
    monkeypatch.setattr(config, "EXPLAIN_DAILY_LIMIT", 2)


@pytest.fixture
def fake_explain(monkeypatch):
    import app as app_module
    calls = []

    def run(facts, mode):
        calls.append((facts, mode))
        return {"answer": {"summary": "Итог.", "points": [], "limits": "—"}, "problems": [],
                "attempts": 1, "usage": {}, "model": "claude-haiku-5-5"}
    monkeypatch.setattr(app_module, "explain", run)
    return calls


def _user_id(fake_db, name="tester"):
    return fake_db.get_user_by_username(name)["id"]


def test_it_is_off_without_a_key(client, auth, monkeypatch):
    monkeypatch.setattr(config, "EXPLAIN_ENABLED", False)
    headers = auth()
    assert client.get("/api/analytics/explain/status", headers=headers).json()["enabled"] is False
    assert client.post("/api/analytics/explain", json={"mode": "clan"}, headers=headers).status_code == 503


def test_a_mode_and_enough_battles_are_required(client, auth, fake_db, enabled, fake_explain):
    headers = auth()
    assert client.post("/api/analytics/explain", json={}, headers=headers).status_code == 400
    _seed(fake_db, _user_id(fake_db), n=4)
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 400 and "10" in res.json()["detail"]
    assert not fake_explain


def test_the_same_period_is_answered_from_the_cache(client, auth, fake_db, enabled, fake_explain):
    headers = auth()
    _seed(fake_db, _user_id(fake_db))
    first = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    again = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert first.status_code == again.status_code == 200
    assert (first.json()["cached"], again.json()["cached"]) == (False, True)
    assert len(fake_explain) == 1
    facts, mode = fake_explain[0]
    assert mode == "clan" and "Me" not in json.dumps(facts, ensure_ascii=False), "no nicknames leave"
    assert client.get("/api/analytics/explain/status", headers=headers).json()["used"] == 1


def test_the_daily_limit_counts_new_analyses_and_failures(client, auth, fake_db, enabled, monkeypatch):
    import app as app_module
    headers = auth()
    _seed(fake_db, _user_id(fake_db))

    def fail(facts, mode):
        raise explainer.ExplainError("Разбор временно недоступен")
    monkeypatch.setattr(app_module, "explain", fail)
    for last_day in ("2026-11-08", "2026-11-09"):
        res = client.post("/api/analytics/explain", headers=headers, json={
            "mode": "clan", "scope": "mine", "date_from": "2026-11-01", "date_to": last_day})
        assert res.status_code == 502 and "недоступен" in res.json()["detail"]
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 429, "two failed attempts used up the limit of two"


# --- the model call ---

def _response(text, stop="end_turn"):
    return SimpleNamespace(stop_reason=stop, model="claude-haiku-5-5",
                           content=[SimpleNamespace(type="text", text=text)],
                           usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_read_input_tokens=0))


FACTS = {"battles": 40, "players": [{"id": "ты", "battles": 40}],
         "facts": [{"id": "f1", "about": "ты", "metric": "damage_vs_norm", "value": 18, "n": 40,
                    "confidence": "надёжно"}]}
GOOD = {"summary": "Урон выше нормы.", "limits": "Позиций нет.", "points": [
    {"kind": "сильная сторона", "about": "ты", "text": "Урон на 18% выше нормы.", "facts": ["f1"]},
    {"kind": "наблюдение", "about": "ты", "text": "Боёв 40.", "facts": ["f1"]},
    {"kind": "наблюдение", "about": "ты", "text": "Других фактов нет.", "facts": ["f1"]}]}


def _fake_client(monkeypatch, replies):
    sent = []

    class Messages:
        def create(self, **kwargs):
            sent.append(kwargs)
            return replies.pop(0)
    monkeypatch.setattr(explainer, "_get_client", lambda: SimpleNamespace(messages=Messages()))
    return sent


def test_json_is_found_inside_a_fence_or_text():
    body = json.dumps(GOOD, ensure_ascii=False)
    assert json.loads(explainer.extract_json(f"```json\n{body}\n```")) == GOOD
    assert json.loads(explainer.extract_json(f"Вот разбор:\n{body}\nГотово.")) == GOOD


def test_a_broken_answer_gets_one_retry_with_the_problems(monkeypatch):
    bad = {**GOOD, "points": [{**GOOD["points"][0], "text": "Урон на 25% выше нормы."}, *GOOD["points"][1:]]}
    sent = _fake_client(monkeypatch, [_response(json.dumps(bad, ensure_ascii=False)),
                                      _response("```json\n" + json.dumps(GOOD, ensure_ascii=False) + "\n```")])
    result = explainer.explain(FACTS, "standard")
    assert result["answer"] == GOOD and result["attempts"] == 2
    retry = sent[1]["messages"]
    assert [m["role"] for m in retry] == ["user", "assistant", "user"] and "числа 25" in retry[-1]["content"]
    assert "## Режим: стандартный бой" in sent[0]["system"][0]["text"]
    assert sent[0]["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_still_broken_or_refused_is_an_error(monkeypatch):
    _fake_client(monkeypatch, [_response("не JSON"), _response("опять не JSON")])
    with pytest.raises(explainer.ExplainError):
        explainer.explain(FACTS, "standard")
    _fake_client(monkeypatch, [_response("", stop="refusal")])
    with pytest.raises(explainer.ExplainError, match="отказалась"):
        explainer.explain(FACTS, "standard")


def test_it_needs_the_current_consent(client, auth, fake_db, enabled, fake_explain):
    headers = auth()
    uid = _user_id(fake_db)
    _seed(fake_db, uid)
    fake_db.record_consent(uid, "2026-11-09")  # the text before the AI analysis was described
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 403 and not fake_explain


def test_a_second_request_in_flight_and_the_site_wide_cap_are_refused(client, auth, fake_db, enabled,
                                                                       fake_explain, monkeypatch):
    import app as app_module
    headers = auth()
    uid = _user_id(fake_db)
    _seed(fake_db, uid)
    monkeypatch.setattr(app_module, "_explaining", {uid})
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 429 and "готовится" in res.json()["detail"]

    monkeypatch.setattr(app_module, "_explaining", set())
    monkeypatch.setattr(config, "EXPLAIN_GLOBAL_DAILY_LIMIT", 0)
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 429 and "завтра" in res.json()["detail"]
    assert not fake_explain and not app_module._explaining
