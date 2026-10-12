"""API-key hashing (Argon2id) and outbound URL guards. Spec §8."""

from __future__ import annotations

import ipaddress
import secrets
import socket
from urllib.parse import urlparse

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

from app.core.exceptions import ValidationFailed

__all__ = ["ensure_safe_url", "generate_api_key", "hash_api_key", "verify_api_key"]

_hasher = PasswordHasher()


def generate_api_key() -> str:
    return "d2f_" + secrets.token_urlsafe(32)


def hash_api_key(key: str) -> str:
    return _hasher.hash(key)


def verify_api_key(key: str, hashes: list[str]) -> bool:
    for h in hashes:
        try:
            if _hasher.verify(h, key):
                return True
        except (VerificationError, InvalidHashError):
            continue
    return False


def _is_private(host: str) -> bool:
    try:
        candidates = [ipaddress.ip_address(host)]
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, None)
        except OSError:
            return False  # unresolvable: the request itself will fail, nothing to protect
        candidates = [ipaddress.ip_address(i[4][0]) for i in infos]
    return any(
        a.is_private or a.is_loopback or a.is_link_local or a.is_reserved for a in candidates
    )


def ensure_safe_url(url: str, *, field: str, allow_private: bool) -> str:
    """Normalise a user-supplied base URL and refuse non-https or internal hosts.

    Without this a caller could point "Databricks host" at an internal service and use the
    migration API as a proxy into the network the service runs in.
    """
    raw = (url or "").strip().rstrip("/")
    if raw and "://" not in raw:
        raw = "https://" + raw
    p = urlparse(raw)
    if p.scheme not in ("https", "http") or not p.hostname:
        raise ValidationFailed(f"{field} must be an https URL", field=field)
    if allow_private:
        return raw
    if p.scheme != "https":
        raise ValidationFailed(f"{field} must use https", field=field)
    if _is_private(p.hostname):
        raise ValidationFailed(f"{field} resolves to a private or loopback address", field=field)
    return raw
