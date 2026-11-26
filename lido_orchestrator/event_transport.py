"""Small authenticated event envelope helpers for explicitly injected transports.

No network client or credential lookup is provided. Callers supply key material.
Payload fields, including email content, are untrusted data and never executable
instructions to this module.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

MAX_ENVELOPE_BYTES = 64_000
MAX_CONTENT_CHARS = 12_000


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_envelope(event_id: str, event_type: str, payload: dict[str, Any], key: bytes, *, issued_at: int | None = None) -> dict[str, Any]:
    if len(key) < 32: raise ValueError("key must contain at least 32 bytes")
    if not event_id or len(event_id) > 200 or not event_type or len(event_type) > 100:
        raise ValueError("invalid event identity")
    if not isinstance(payload, dict): raise ValueError("payload must be an object")
    _validate_untrusted_content(payload)
    body = {"event_id": event_id, "event_type": event_type, "issued_at": int(time.time() if issued_at is None else issued_at), "payload": payload}
    if len(_canonical(body)) > MAX_ENVELOPE_BYTES: raise ValueError("event envelope exceeds size limit")
    return {**body, "signature": hmac.new(key, _canonical(body), hashlib.sha256).hexdigest()}


def verify_envelope(envelope: dict[str, Any], key: bytes, *, now: int | None = None, max_age: int = 300) -> dict[str, Any]:
    if not isinstance(envelope, dict) or set(envelope) != {"event_id", "event_type", "issued_at", "payload", "signature"}:
        raise ValueError("invalid event envelope fields")
    if len(key) < 32: raise ValueError("key must contain at least 32 bytes")
    signature = envelope.get("signature")
    body = {k: envelope[k] for k in ("event_id", "event_type", "issued_at", "payload")}
    if not isinstance(signature, str) or len(signature) != 64 or not hmac.compare_digest(signature, hmac.new(key, _canonical(body), hashlib.sha256).hexdigest()):
        raise ValueError("event authentication failed")
    if len(_canonical(envelope)) > MAX_ENVELOPE_BYTES: raise ValueError("event envelope exceeds size limit")
    if not isinstance(body["issued_at"], int) or abs((int(time.time() if now is None else now)) - body["issued_at"]) > max_age:
        raise ValueError("event envelope is outside the accepted time window")
    if not isinstance(body["event_id"], str) or not body["event_id"] or len(body["event_id"]) > 200:
        raise ValueError("invalid event identity")
    if not isinstance(body["event_type"], str) or not body["event_type"] or len(body["event_type"]) > 100:
        raise ValueError("invalid event type")
    if not isinstance(body["payload"], dict): raise ValueError("payload must be an object")
    _validate_untrusted_content(body["payload"])
    return body


def _validate_untrusted_content(payload: dict[str, Any]) -> None:
    """Bound optional source/body fields; do not parse or execute their contents."""
    for field in ("content", "email_body", "subject"):
        value = payload.get(field)
        if value is not None and (not isinstance(value, str) or len(value) > MAX_CONTENT_CHARS):
            raise ValueError(f"{field} must be bounded text")

