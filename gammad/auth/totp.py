"""OWNER EDITION ONLY: RFC 6238 TOTP over the RFC 4226 HOTP core, stdlib hmac only (no pyotp; BROKER §5, §7).

D86: in the CLIENT edition broker login is never automated. This module refuses to import there
(`edition.require_owner` below raises `OwnerOnlyError`) and every public function re-checks at call time, so a
module object imported earlier in the process cannot be used after the edition says client.

    totp(secret, at=t, step=30, digits=6, algorithm="sha1", t0=0)
        counter = floor((t - t0) / step)                       RFC 6238 §4.2
        code    = HOTP(K, counter) = truncate(HMAC(K, counter as 8-byte big-endian)) mod 10**digits
                                                               RFC 4226 §5.3 (dynamic truncation, 31-bit)
Secrets arrive base32 (as brokers print them; spaces, dashes, lower case and missing '=' padding tolerated) or as
raw bytes (the RFC test seeds). Secret values and codes are never logged.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import struct
import time

from gammad.edition import require_owner

require_owner("gammad.auth.totp (automated broker login)")

ALGORITHMS = {"sha1": hashlib.sha1, "sha256": hashlib.sha256, "sha512": hashlib.sha512}
DEFAULT_STEP = 30
DEFAULT_DIGITS = 6


def decode_secret(secret: str | bytes) -> bytes:
    """A base32 text secret -> key bytes; bytes pass through unchanged. ValueError on bad base32."""
    require_owner("gammad.auth.totp")
    if isinstance(secret, bytes | bytearray):
        return bytes(secret)
    text = "".join(ch for ch in secret if ch not in " -\t\r\n").upper().rstrip("=")
    if not text:
        raise ValueError("empty TOTP secret")
    text += "=" * (-len(text) % 8)
    try:
        return base64.b32decode(text, casefold=False)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("TOTP secret is not valid base32") from exc


def hotp(key: bytes, counter: int, *, digits: int = DEFAULT_DIGITS, algorithm: str = "sha1") -> str:
    """RFC 4226 HOTP value, zero-padded to `digits`."""
    require_owner("gammad.auth.totp")
    if algorithm not in ALGORITHMS:
        raise ValueError(f"algorithm must be one of {sorted(ALGORITHMS)}")
    if not 6 <= digits <= 10:
        raise ValueError("digits must be 6..10")
    if counter < 0:
        raise ValueError("counter must be >= 0")
    mac = hmac.new(key, struct.pack(">Q", counter), ALGORITHMS[algorithm]).digest()
    offset = mac[-1] & 0x0F
    binary = struct.unpack(">I", mac[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(binary % (10**digits)).zfill(digits)


def time_counter(at: float, *, step: int = DEFAULT_STEP, t0: int = 0) -> int:
    """RFC 6238 T = floor((Unix time - T0) / X)."""
    require_owner("gammad.auth.totp")
    if step <= 0:
        raise ValueError("step must be > 0")
    return int((at - t0) // step)


def totp(
    secret: str | bytes,
    *,
    at: float | None = None,
    step: int = DEFAULT_STEP,
    digits: int = DEFAULT_DIGITS,
    algorithm: str = "sha1",
    t0: int = 0,
) -> str:
    """The TOTP code for `secret` at Unix time `at` (default: now)."""
    require_owner("gammad.auth.totp")
    now = time.time() if at is None else float(at)
    return hotp(decode_secret(secret), time_counter(now, step=step, t0=t0), digits=digits, algorithm=algorithm)


def seconds_left(at: float | None = None, *, step: int = DEFAULT_STEP, t0: int = 0) -> float:
    """Seconds until the current code rolls over (a caller may wait rather than submit a code about to expire)."""
    require_owner("gammad.auth.totp")
    now = time.time() if at is None else float(at)
    return step - ((now - t0) % step)
