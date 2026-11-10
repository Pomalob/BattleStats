"""The language model that puts a period's facts into words.

One Messages API call per analysis, through GPTunnel's Anthropic-compatible endpoint with the
official SDK: the mode's prompt as the system prompt (cached: it is the same for every user),
the facts as JSON in the user turn. The answer is checked against the facts (explain_check.py);
if it fails, the model gets the list of problems once and tries again. Nothing but the
anonymised facts is sent.

GPTunnel strips `output_config` (the JSON schema) and `thinking` on Haiku models, so the
answer's JSON is not guaranteed by the API: the prompt asks for it, `extract_json` digs it out
of whatever came back, and a malformed answer counts as a failed check.

Every failure is an ExplainError with a `kind` (payment, auth, model, ...) and a `detail` with
what the provider said: the user gets a message that names the cause, the admin page the rest.
"""
import json
import re

import anthropic

import config
import explain_check
from insights import prompt_for

MAX_TOKENS = 8000
ATTEMPTS = 2

# Kinds of failure, as the admin page names them.
KINDS = {
    "payment": "не оплачен баланс сервиса",
    "auth": "неверный или просроченный ключ",
    "model": "модель недоступна для ключа",
    "rate_limit": "сервис перегружен",
    "timeout": "модель не ответила вовремя",
    "connection": "нет связи с сервисом",
    "provider": "сбой на стороне сервиса",
    "bad_request": "сервис отклонил запрос",
    "api": "ошибка API",
    "refusal": "модель отказалась отвечать",
    "too_long": "ответ не уместился",
    "check_failed": "ответ не прошёл проверку по цифрам",
}
# What the user is told. What is under the hood (the router, its balance, the key, the model)
# is none of their business: only the admin page shows the cause.
UNAVAILABLE = "Разбор временно недоступен, попробуй позже"
MESSAGES = {kind: UNAVAILABLE for kind in KINDS}
MESSAGES["too_long"] = "Разбор получился слишком длинным, попробуй период покороче"
_PAYMENT = re.compile(r"balance|insufficient|funds|payment|billing|credit|top.?up|"
                      r"недостаточно|баланс|оплат|средств|пополн", re.I)
_MODEL = re.compile(r"model", re.I)


class ExplainError(Exception):
    """The analysis could not be produced. str() is safe to show to the user; `kind` and
    `detail` are for the admin log."""

    def __init__(self, kind: str, detail: dict | None = None):
        super().__init__(MESSAGES.get(kind, UNAVAILABLE))
        self.kind = kind
        self.detail = detail or {}


_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(
            api_key=config.EXPLAIN_API_KEY,
            base_url=config.EXPLAIN_BASE_URL,
            timeout=float(config.EXPLAIN_TIMEOUT_SECONDS),
            # A request holds a server thread: keep the worst case to two timeouts per attempt.
            max_retries=1,
        )
    return _client


def hard_problems(problems: list[str]) -> list[str]:
    """The problems that make an answer wrong, as opposed to warnings worth a look."""
    return [p for p in problems if not p.startswith("предупреждение")]


def extract_json(text: str) -> str:
    """The JSON object in a reply: as is, inside a ```json fence, or between the outer braces."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("```", 1)[0]
    start, end = text.find("{"), text.rfind("}")
    return text[start:end + 1] if start != -1 and end > start else text


def _provider_message(e: anthropic.APIError) -> str:
    """What the provider said, from the error body if it has one. Never the request itself."""
    body = getattr(e, "body", None)
    if isinstance(body, dict):
        err = body.get("error") if isinstance(body.get("error"), dict) else body
        text = err.get("message") or err.get("detail") or json.dumps(body, ensure_ascii=False)
    else:
        text = str(body or getattr(e, "message", "") or e)
    return str(text)[:500]


def classify(e: anthropic.APIError) -> ExplainError:
    """An SDK error as an ExplainError of the kind that names the cause."""
    if isinstance(e, anthropic.APITimeoutError):
        return ExplainError("timeout", {"error": type(e).__name__})
    if isinstance(e, anthropic.APIConnectionError):
        return ExplainError("connection", {"error": type(e).__name__, "message": str(e)[:300]})
    status = getattr(e, "status_code", None)
    message = _provider_message(e)
    detail = {"status": status, "type": getattr(e, "type", None), "message": message,
              "request_id": getattr(e, "request_id", None)}
    if status == 402 or _PAYMENT.search(message):
        kind = "payment"
    elif status == 401:
        kind = "auth"
    elif status == 404 or (status == 403 and _MODEL.search(message)):
        kind = "model"
    elif status == 429:
        kind = "rate_limit"
    elif status == 400:
        kind = "bad_request"
    elif status is not None and status >= 500:
        kind = "provider"
    else:
        kind = "api"
    return ExplainError(kind, detail)


def _create(**kwargs):
    try:
        return _get_client().messages.create(model=config.EXPLAIN_MODEL, **kwargs)
    except anthropic.APIError as e:
        error = classify(e)
        print(f"[explain] {error.kind}: {error.detail}")
        raise error


def _ask(messages: list[dict], mode: str):
    return _create(
        max_tokens=MAX_TOKENS,
        # The prompt is the same for every period of this mode: cache it. GPTunnel passes
        # cache_control on only where prompt caching is switched on for the key.
        system=[{"type": "text", "text": prompt_for(mode), "cache_control": {"type": "ephemeral"}}],
        messages=messages,
    )


def _add_usage(total: dict, usage) -> None:
    """Every number the provider reports, summed over attempts (GPTunnel may add a price)."""
    data = usage.to_dict() if hasattr(usage, "to_dict") else dict(vars(usage))
    for key, value in data.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total[key] = total.get(key, 0) + value


def explain(facts: dict, mode: str) -> dict:
    """The checked answer for these facts: {"answer", "problems", "attempts", "usage", "model"}.

    `problems` holds only warnings when it returns. Raises ExplainError when the call fails,
    the model refuses or gets cut off, or the answer still breaks the rules after the retry."""
    messages = [{"role": "user", "content": json.dumps(facts, ensure_ascii=False)}]
    usage: dict = {}
    problems: list[str] = []
    for attempt in range(1, ATTEMPTS + 1):
        response = _ask(messages, mode)
        _add_usage(usage, response.usage)
        if response.stop_reason == "refusal":
            raise ExplainError("refusal", {"usage": usage})
        if response.stop_reason == "max_tokens":
            raise ExplainError("too_long", {"usage": usage})
        raw = next((b.text for b in response.content if b.type == "text"), "")
        text = extract_json(raw)
        problems = explain_check.check(text, facts)
        if not hard_problems(problems):
            return {"answer": json.loads(text), "problems": problems, "attempts": attempt,
                    "usage": usage, "model": response.model or config.EXPLAIN_MODEL}
        # One more go, with what was wrong. Appending keeps the history as the model wrote it.
        messages += [
            {"role": "assistant", "content": raw or "{}"},
            {"role": "user", "content": "Ответ нарушает правила. Исправь и верни только JSON целиком, "
                                        "без текста вокруг:\n- " + "\n- ".join(hard_problems(problems))},
        ]
    print(f"[explain] answer failed the check after {ATTEMPTS} attempts: {hard_problems(problems)}")
    raise ExplainError("check_failed", {"problems": hard_problems(problems)[:10], "usage": usage})


def ping() -> dict:
    """A tiny request to see that the key, the balance and the model work. For the admin page."""
    response = _create(max_tokens=16, messages=[{"role": "user", "content": "Ответь одним словом: работает"}])
    usage: dict = {}
    _add_usage(usage, response.usage)
    text = next((b.text for b in response.content if b.type == "text"), "")
    return {"ok": True, "model": response.model, "reply": text[:100], "usage": usage}
