"""Battle tickets: the server's own parse of a replay, signed, for the client to hand back.

/api/upload parses replays but stores nothing; the user then picks which battles to save.
If /api/battles/save took the battle as JSON, anyone could post invented numbers, and the
first upload of a battle_hash wins forever, so one forged copy would stick for everybody.
Instead upload returns, per battle, a ticket: the parsed battle compressed and signed with
an HMAC of SECRET_KEY. Save accepts tickets only, so what reaches the database is exactly
what the server read out of a replay file.

A ticket is an opaque string to the browser. Signing the JSON itself would not survive the
round trip: JavaScript rounds 17-digit arenaUniqueIDs and rewrites 1.0 as 1.
"""
import base64
import hashlib
import hmac
import json
import time
import zlib

from config import BATTLE_TICKET_TTL_HOURS, SECRET_KEY

_VERSION = "1"
# A separate key per purpose: a ticket signature is never valid as anything else.
_KEY = hmac.new(SECRET_KEY.encode(), b"battlestats/battle-ticket/v1", hashlib.sha256).digest()
# A parsed battle compresses to a few kilobytes; these only bound hostile input.
MAX_TICKET_CHARS = 1_000_000
_MAX_INFLATED = 8 * 1024 * 1024


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _sign(body: bytes) -> bytes:
    return hmac.new(_KEY, body, hashlib.sha256).digest()


def issue(battle: dict, now: float | None = None) -> str:
    payload = json.dumps({"t": int(now if now is not None else time.time()), "b": battle},
                         ensure_ascii=False, separators=(",", ":")).encode()
    body = zlib.compress(payload, 6)
    return f"{_VERSION}.{_b64(body)}.{_b64(_sign(body))}"


def open_ticket(ticket, now: float | None = None) -> dict | None:
    """The battle inside a genuine, unexpired ticket; None for anything else."""
    if not isinstance(ticket, str) or len(ticket) > MAX_TICKET_CHARS:
        return None
    try:
        version, body_text, sig_text = ticket.split(".")
        if version != _VERSION:
            return None
        body, signature = _unb64(body_text), _unb64(sig_text)
    except ValueError:
        return None
    # Checked before anything is decompressed or parsed.
    if not hmac.compare_digest(signature, _sign(body)):
        return None
    try:
        inflater = zlib.decompressobj()
        payload = inflater.decompress(body, _MAX_INFLATED)
        if inflater.unconsumed_tail:
            return None
        data = json.loads(payload)
    except (zlib.error, ValueError):
        return None
    age = (now if now is not None else time.time()) - data.get("t", 0)
    if not 0 <= age <= BATTLE_TICKET_TTL_HOURS * 3600:
        return None
    battle = data.get("b")
    return battle if isinstance(battle, dict) else None
