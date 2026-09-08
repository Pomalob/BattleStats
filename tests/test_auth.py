from datetime import datetime, timedelta, timezone

import jwt
import pytest

import config
from auth import create_token, decode_token, hash_password, verify_password


def test_password_round_trip():
    hashed = hash_password("correct horse")
    assert hashed != "correct horse"
    assert verify_password("correct horse", hashed)
    assert not verify_password("wrong horse", hashed)


def test_passwords_longer_than_the_bcrypt_limit_are_accepted():
    """bcrypt only reads 72 bytes and newer releases reject longer input outright."""
    long_password = "x" * 200
    assert verify_password(long_password, hash_password(long_password))


def test_unicode_password_round_trip():
    assert verify_password("парольчик", hash_password("парольчик"))


def test_malformed_stored_hash_is_a_failed_login_not_a_crash():
    assert verify_password("anything", "not-a-bcrypt-hash") is False


def test_token_round_trip():
    payload = decode_token(create_token(42, "tester"))
    assert payload["sub"] == "42"
    assert payload["username"] == "tester"


@pytest.mark.parametrize("token", ["", "garbage", "a.b.c"])
def test_invalid_tokens_are_rejected(token):
    assert decode_token(token) is None


def test_expired_token_is_rejected():
    expired = jwt.encode(
        {"sub": "1", "username": "tester",
         "exp": datetime.now(timezone.utc) - timedelta(seconds=1)},
        config.SECRET_KEY, algorithm="HS256",
    )
    assert decode_token(expired) is None


def test_token_signed_with_another_key_is_rejected():
    forged = jwt.encode({"sub": "1", "username": "admin"}, "some-other-key", algorithm="HS256")
    assert decode_token(forged) is None


def test_unsigned_token_is_rejected():
    """A `none`-algorithm token must never be accepted."""
    unsigned = jwt.encode({"sub": "1", "username": "admin"}, key=None, algorithm="none")
    assert decode_token(unsigned) is None
