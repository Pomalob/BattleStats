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
        raise explainer.ExplainError("provider")
    monkeypatch.setattr(app_module, "explain", fail)
    for last_day in ("2026-11-08", "2026-11-09"):
        res = client.post("/api/analytics/explain", headers=headers, json={
            "mode": "clan", "scope": "mine", "date_from": "2026-11-01", "date_to": last_day})
        assert res.status_code == 502 and res.json()["detail"] == explainer.MESSAGES["provider"]
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
    with pytest.raises(explainer.ExplainError) as refused:
        explainer.explain(FACTS, "standard")
    assert refused.value.kind == "refusal" and str(refused.value) == explainer.UNAVAILABLE


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


# --- causes of failure ---

def _status_error(status, body):
    import anthropic
    import httpx2
    response = httpx2.Response(status, json=body, request=httpx2.Request("POST", "https://gptunnel.ru/v1/messages"),
                               headers={"request-id": "req_1"})
    return anthropic.APIStatusError("error", response=response, body=body)


@pytest.mark.parametrize("status, body, kind", [
    (402, {"error": {"type": "payment_required", "message": "Payment required"}}, "payment"),
    (403, {"error": {"type": "permission_error", "message": "Insufficient balance"}}, "payment"),
    (400, {"code": 19, "message": "Недостаточно средств на балансе"}, "payment"),
    (401, {"error": {"type": "unauthorized_request_error", "message": "API key expired", "code": "api_key_expired"}}, "auth"),
    (403, {"error": {"type": "permission_error", "message": "Model 'claude-x' is not allowed for this API key."}}, "model"),
    (404, {"error": {"type": "not_found_error", "message": "not found"}}, "model"),
    (429, {"error": {"type": "rate_limit_error", "message": "slow down"}}, "rate_limit"),
    (503, {"error": {"type": "overloaded_error", "message": "busy"}}, "provider"),
])
def test_provider_errors_name_their_cause(status, body, kind):
    error = explainer.classify(_status_error(status, body))
    assert error.kind == kind
    assert error.detail["status"] == status and error.detail["request_id"] == "req_1"
    assert error.detail["message"] and str(error) == explainer.UNAVAILABLE, "users never see the cause"


def test_a_failure_is_logged_with_its_cause_and_admins_see_the_details(client, auth, fake_db, enabled,
                                                                       monkeypatch):
    import app as app_module
    headers = auth()
    uid = _user_id(fake_db)
    _seed(fake_db, uid)

    def unpaid(facts, mode):
        raise explainer.classify(_status_error(402, {"error": {"message": "Insufficient balance"}}))
    monkeypatch.setattr(app_module, "explain", unpaid)
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    assert res.status_code == 502 and res.json()["detail"] == explainer.UNAVAILABLE
    logged = fake_db.explanations[-1]
    assert (logged["status"], logged["error_kind"]) == ("failed", "payment")
    assert logged["error"]["message"] == "Insufficient balance" and logged["duration_ms"] is not None

    assert client.get("/api/admin/explanations", headers=headers).status_code == 403
    monkeypatch.setattr(config, "ADMIN_USER_IDS", frozenset({uid}))
    res = client.post("/api/analytics/explain", json={"mode": "clan", "scope": "mine"}, headers=headers)
    detail = res.json()["detail"]
    assert "Insufficient balance" in detail and explainer.KINDS["payment"] in detail, "the admin sees the cause"
    log = client.get("/api/admin/explanations", headers=headers).json()
    assert log["settings"]["model"] == config.EXPLAIN_MODEL and log["recent"][0]["error_kind"] == "payment"
    monkeypatch.setattr(config, "EXPLAIN_API_KEY", "secret-key-value")
    assert "secret-key-value" not in client.get("/api/admin/explanations", headers=headers).text


def test_the_admin_check_reports_the_cause(client, auth, fake_db, enabled, monkeypatch):
    headers = auth()
    monkeypatch.setattr(config, "ADMIN_USER_IDS", frozenset({_user_id(fake_db)}))
    monkeypatch.setattr(explainer, "ping", lambda: (_ for _ in ()).throw(
        explainer.classify(_status_error(402, {"error": {"message": "Payment required"}}))))
    res = client.post("/api/admin/explanations/check", headers=headers).json()
    assert (res["ok"], res["kind"]) == (False, "payment") and res["detail"]["status"] == 402
    monkeypatch.setattr(explainer, "ping", lambda: {"ok": True, "model": "m", "reply": "работает", "usage": {}})
    assert client.post("/api/admin/explanations/check", headers=headers).json()["ok"] is True
