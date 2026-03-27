"""
Token encryption helpers for sensitive fields (Strava OAuth tokens).

Uses Fernet symmetric encryption (AES-128-CBC + HMAC-SHA256).

Key resolution order:
  1. TOKEN_ENCRYPTION_KEY env var — a URL-safe base64-encoded 32-byte key.
     Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  2. Derived from SECRET_KEY via PBKDF2-HMAC-SHA256 with a fixed app salt.
     Safe as long as SECRET_KEY is never rotated without also re-encrypting tokens.

Usage:
    from .crypto import encrypt_token, decrypt_token

    stored  = encrypt_token("raw_access_token")
    raw     = decrypt_token(stored)          # returns original string
    raw     = decrypt_token("plaintext")     # gracefully returns plaintext (migration period)
"""

from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken


# Fixed salt — not a secret, just ensures domain separation from other
# SECRET_KEY usages (e.g. Flask sessions).
_DERIVE_SALT = b"strideiq-token-encryption-v1"


def _derive_fernet_from_secret() -> Fernet:
    """Derive a valid Fernet key from SECRET_KEY using PBKDF2."""
    secret = os.environ.get("SECRET_KEY", "")
    if not secret:
        raise RuntimeError(
            "Cannot encrypt tokens: neither TOKEN_ENCRYPTION_KEY nor SECRET_KEY "
            "is set in the environment."
        )
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode(),
        _DERIVE_SALT,
        iterations=100_000,
        dklen=32,
    )
    return Fernet(base64.urlsafe_b64encode(derived))


def _build_fernet() -> Fernet:
    """Return a Fernet instance.

    Resolution order:
    1. TOKEN_ENCRYPTION_KEY env var if it is a valid Fernet key.
    2. Derived from SECRET_KEY via PBKDF2 (fallback — works out of the box,
       but key rotation requires a re-encryption step).

    If TOKEN_ENCRYPTION_KEY is set but is not a valid Fernet key (e.g. Render
    generated a random string instead of a proper base64 key), we log a warning
    and fall back to derivation so the app never crashes due to a bad key value.
    """
    import logging
    raw_key = os.environ.get("TOKEN_ENCRYPTION_KEY", "").strip()
    if raw_key:
        try:
            return Fernet(raw_key.encode())
        except Exception:
            logging.getLogger(__name__).warning(
                "TOKEN_ENCRYPTION_KEY is set but is not a valid Fernet key "
                "(must be 32 url-safe base64-encoded bytes). "
                "Falling back to SECRET_KEY derivation. "
                "Regenerate with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )

    return _derive_fernet_from_secret()


def encrypt_token(plaintext: str) -> str:
    """Encrypt a token string. Returns a Fernet token string (URL-safe base64)."""
    if not plaintext:
        return plaintext
    fernet = _build_fernet()
    return fernet.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted token string.

    Falls back to returning the value as-is if decryption fails — this handles
    the migration window where existing rows still hold plaintext tokens.
    Once migration 005 has run, all tokens are encrypted and this fallback
    becomes unreachable.
    """
    if not ciphertext:
        return ciphertext
    try:
        fernet = _build_fernet()
        return fernet.decrypt(ciphertext.encode()).decode()
    except (InvalidToken, Exception):
        # Value is plaintext (pre-encryption) — return as-is.
        return ciphertext
