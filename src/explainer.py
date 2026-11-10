"""The language model that puts a period's facts into words.

One Messages API call per analysis, through GPTunnel's Anthropic-compatible endpoint with the
official SDK: the mode's prompt as the system prompt (cached: it is the same for every user),
the facts as JSON in the user turn. The answer is checked against the facts (explain_check.py);
if it fails, the model gets the list of problems once and tries again. Nothing but the
anonymised facts is sent.

GPTunnel strips `output_config` (the JSON schema) and `thinking` on Haiku models, so the
answer's JSON is not guaranteed by the API: the prompt asks for it, `extract_json` digs it out
of whatever came back, and a malformed answer counts as a failed check.
"""
import json

import anthropic

import config
import explain_check
from insights import prompt_for

MAX_TOKENS = 8000
ATTEMPTS = 2


class ExplainError(Exception):
    """The analysis could not be produced; the message is safe to show to the user."""


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


def _ask(messages: list[dict], mode: str):
    try:
        return _get_client().messages.create(
            model=config.EXPLAIN_MODEL,
            max_tokens=MAX_TOKENS,
            # The prompt is the same for every period of this mode: cache it. GPTunnel passes
            # cache_control on only where prompt caching is switched on for the key.
            system=[{"type": "text", "text": prompt_for(mode), "cache_control": {"type": "ephemeral"}}],
            messages=messages,
        )
    except anthropic.RateLimitError:
        raise ExplainError("Сервис разбора сейчас перегружен, попробуй через минуту")
    except anthropic.APITimeoutError:
        raise ExplainError("Разбор не успел за отведённое время, попробуй ещё раз")
    except (anthropic.AuthenticationError, anthropic.PermissionDeniedError, anthropic.NotFoundError,
            anthropic.BadRequestError) as e:
        # A configuration problem on our side (key, model not allowed, unknown model): log it.
        print(f"[explain] request rejected: {type(e).__name__}: {e}")
        raise ExplainError("Разбор временно недоступен")
    except anthropic.APIStatusError as e:
        print(f"[explain] API error {e.status_code}: {e}")
        raise ExplainError("Разбор временно недоступен, попробуй позже")
    except anthropic.APIConnectionError:
        raise ExplainError("Нет связи с сервисом разбора, попробуй позже")


def explain(facts: dict, mode: str) -> dict:
    """The checked answer for these facts: {"answer", "problems", "attempts", "usage", "model"}.

    `problems` holds only warnings when it returns. Raises ExplainError when the model
    refuses, gets cut off, or still breaks the rules after the retry."""
    messages = [{"role": "user", "content": json.dumps(facts, ensure_ascii=False)}]
    usage = {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0}
    problems: list[str] = []
    for attempt in range(1, ATTEMPTS + 1):
        response = _ask(messages, mode)
        for key in usage:
            usage[key] += getattr(response.usage, key, 0) or 0
        if response.stop_reason == "refusal":
            raise ExplainError("Модель отказалась разбирать этот период")
        if response.stop_reason == "max_tokens":
            raise ExplainError("Разбор получился слишком длинным, попробуй период покороче")
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
    raise ExplainError("Не удалось получить разбор, который сходится с цифрами. Попробуй ещё раз")
