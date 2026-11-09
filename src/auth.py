import bcrypt
import jwt
from datetime import datetime, timedelta, timezone

from config import SECRET_KEY, TOKEN_TTL_DAYS

_ALGORITHM = "HS256"
# bcrypt only reads the first 72 bytes of a password and newer releases reject
# anything longer instead of truncating, so truncate here for both paths.
_BCRYPT_MAX_BYTES = 72


def _pw_bytes(password: str) -> bytes:
    return password.encode()[:_BCRYPT_MAX_BYTES]


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_pw_bytes(password), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_pw_bytes(password), hashed.encode())
    except ValueError:
        # Malformed hash in the DB — treat as a failed login, not a 500.
        return False


def create_token(user_id: int, username: str, version: int = 0) -> str:
    """version is the user's token_version: bumping it in the database voids every older token."""
    payload = {
        "sub": str(user_id),
        "username": username,
        "ver": version,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(days=TOKEN_TTL_DAYS),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=_ALGORITHM)


def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[_ALGORITHM])
    except Exception:
        return None
