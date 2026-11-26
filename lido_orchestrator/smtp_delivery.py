"""Opt-in generic SMTP outbox adapter. Configuration is only read when explicitly requested."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import smtplib
import ssl
import time
import os
import stat
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SMTPConfig:
    enabled: bool
    host: str
    port: int
    sender: str
    recipients: tuple[str, ...]
    username: str
    password: str = field(repr=False)
    timeout: float
    min_interval: float
    receipt_key: bytes = field(repr=False)
    subject_prefix: str = "[Lido]"
    security: str = "starttls"


_REQUIRED = {"enabled", "host", "port", "sender", "recipients", "username", "password", "timeout", "min_interval"}
_UPPER_MAP = {"SMTP_HOST":"host", "SMTP_PORT":"port", "SMTP_SECURITY":"security",
              "SMTP_USERNAME":"username", "SMTP_PASSWORD":"password", "SMTP_FROM":"sender",
              "SMTP_TO":"recipients", "SMTP_SUBJECT_PREFIX":"subject_prefix"}


def parse_config(text: str) -> SMTPConfig:
    """Parse strict KEY=VALUE text; no shell syntax, expansion, or error echoing."""
    values: dict[str, str] = {}
    try:
        if not isinstance(text, str) or len(text) > 16_384:
            raise ValueError
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError
            key, value = line.split("=", 1)
            if key == "SMTP_SUBJECT_PREFIX" and len(value)>=2 and value[0]==value[-1] and value[0] in "\"'":
                value=value[1:-1]
            if key not in (_REQUIRED | set(_UPPER_MAP)) or key in values or not value or value != value.strip():
                raise ValueError
            values[key] = value
        # Accept both the phase-9 explicit schema and the established uppercase
        # SMTP settings. The uppercase form deliberately has no enabled switch:
        # the CLI's --enable remains the only invocation opt-in.
        if set(values) == set(_UPPER_MAP):
            mapped = {_UPPER_MAP[k]: v for k, v in values.items()}
            mapped.update(enabled="true", timeout="10", min_interval="2")
            values = mapped
        elif values.keys() != _REQUIRED:
            raise ValueError
        if values["enabled"] not in ("true", "false"):
            raise ValueError
        host, sender = values["host"], values["sender"]
        recipients = tuple(values["recipients"].split(","))
        if (not re.fullmatch(r"(?=.{1,253}\Z)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*", host)
                or not _valid_address(sender) or not recipients or len(recipients) > 50
                or any(not _valid_address(addr) for addr in recipients)):
            raise ValueError
        port = int(values["port"]); timeout = float(values["timeout"]); interval = float(values["min_interval"])
        if not 1 <= port <= 65535 or not 1 <= timeout <= 60 or not 0 <= interval <= 3600:
            raise ValueError
        if len(values["username"]) > 256 or len(values["password"]) > 4096:
            raise ValueError
        key = b""
        security = values.get("security", "starttls").lower()
        prefix = values.get("subject_prefix", "[Lido]")
        if security != "starttls" or not prefix or len(prefix) > 100 or not prefix.isascii() or not prefix.isprintable():
            raise ValueError
        return SMTPConfig(values["enabled"] == "true", host, port, sender, recipients,
                          values["username"], values["password"], timeout, interval, key, prefix, security)
    except Exception:
        raise ValueError("invalid SMTP configuration") from None


def load_config(path: str | Path) -> SMTPConfig:
    """Read only the caller-specified path and report no path or parser detail on failure."""
    try:
        fd=os.open(path,os.O_RDONLY|getattr(os,"O_NOFOLLOW",0)|getattr(os,"O_NONBLOCK",0))
        try:
            st=os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or stat.S_IMODE(st.st_mode)!=0o600 or st.st_size>16_384: raise ValueError
            raw=os.read(fd,16_385)
            if len(raw)>16_384: raise ValueError
        finally: os.close(fd)
        return parse_config(raw.decode("utf-8"))
    except Exception:
        raise ValueError("unable to load SMTP configuration") from None


def load_receipt_key(path: str | Path) -> bytes:
    """Load a caller-generated hex key from a private regular file, without following symlinks."""
    try:
        p = Path(path)
        fd = os.open(p, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o600 or st.st_size > 260:
                raise ValueError
            raw = os.read(fd, 261)
        finally:
            os.close(fd)
        key = bytes.fromhex(raw.decode("ascii").strip())
        if not 32 <= len(key) <= 128: raise ValueError
        return key
    except Exception:
        raise ValueError("unable to load receipt key") from None


def _valid_address(value: str) -> bool:
    if len(value) > 254 or value.count("@") != 1: return False
    local, domain = value.rsplit("@", 1)
    atom = r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+"
    label = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    return bool(re.fullmatch(atom, local) and re.fullmatch(rf"{label}(?:\.{label})*", domain))


def make_receipt(event_id: str, event_type: str, key: bytes, *, now: int | None = None, lifetime: int = 900) -> str:
    if not event_id or len(event_id) > 200 or not event_type or len(event_type) > 100 or len(key) < 32 or not 1 <= lifetime <= 86400:
        raise ValueError("invalid receipt parameters")
    issued = int(time.time() if now is None else now)
    body = {"event_id": event_id, "event_type": event_type, "issued": issued, "expires": issued + lifetime}
    encoded = _b64(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
    sig = _b64(hmac.new(key, encoded.encode("ascii"), hashlib.sha256).digest())
    return encoded + "." + sig


def verify_receipt(token: str, key: bytes, *, now: int | None = None) -> dict[str, Any]:
    try:
        if not isinstance(token, str) or len(token) > 1024 or not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token): raise ValueError
        encoded, signature = token.split(".")
        expected = _b64(hmac.new(key, encoded.encode("ascii"), hashlib.sha256).digest())
        if len(key) < 32 or not hmac.compare_digest(signature, expected): raise ValueError
        body = json.loads(_unb64(encoded))
        current = int(time.time() if now is None else now)
        if (set(body) != {"event_id", "event_type", "issued", "expires"} or not isinstance(body["event_id"], str)
                or not body["event_id"] or len(body["event_id"]) > 200 or not isinstance(body["event_type"], str)
                or not body["event_type"] or len(body["event_type"]) > 100
                or body["expires"] < current or body["issued"] > current + 60 or body["expires"] <= body["issued"]
                or body["expires"] - body["issued"] > 86400): raise ValueError
        body["token_id"] = hashlib.sha256(token.encode()).hexdigest()
        return body
    except Exception:
        raise ValueError("invalid, expired, or unauthenticated receipt") from None


def _b64(raw: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    import base64
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class SMTPOutboxAdapter:
    def __init__(self, engine, config: SMTPConfig, *, clock=time.time, monotonic=time.monotonic, smtp_factory=None):
        self.engine, self.config, self.clock = engine, config, clock
        self.smtp_factory = smtp_factory or smtplib.SMTP

    def deliver_once(self, *, limit: int = 10) -> int:
        if not self.config.enabled:
            return 0
        items = self.engine.claim_outbox(owner="smtp-adapter", limit=1,
            lease_seconds=min(5000, max(60, int(self.config.timeout * 8 + self.config.min_interval + 10))),
            event_types=("worker.escalation", "worker.supervisor_decision"))
        if not items: return 0
        item = items[0]
        slot = self.engine.acquire_smtp_delivery_slot(
            lease_seconds=min(5000, max(60, int(self.config.timeout * 8 + self.config.min_interval + 10))),
            min_interval=self.config.min_interval)
        if slot is None:
            self.engine.release_outbox_claim(item["id"], claim_token=item["lease_token"])
            return 0
        try:
            if slot["wait_seconds"] > 0: time.sleep(slot["wait_seconds"])
            # The one-row event lease covers throttle and SMTP, while the DB
            # slot serializes independent processes sharing this database.
            try:
                self._send(item)
                self.engine.acknowledge_outbox(item["id"], actor="smtp-adapter", claim_token=item["lease_token"])
                return 1
            except Exception:
                try: self.engine.retry_outbox(item["id"], claim_token=item["lease_token"], error="SMTP delivery failed")
                except ValueError: pass
                return 0
        finally:
            self.engine.release_smtp_delivery_slot(slot["token"])

    def _send(self, item: dict[str, Any]) -> None:
        event_id = item["id"]
        receipt = make_receipt(event_id.removeprefix("worker:"), item["event_type"], self.config.receipt_key,
                               now=int(self.clock()))
        msg = EmailMessage()
        msg["From"], msg["To"] = self.config.sender, ", ".join(self.config.recipients)
        msg["Subject"] = f"{self.config.subject_prefix} {item['event_type']}"
        msg["Message-ID"] = f"<{hashlib.sha256(event_id.encode()).hexdigest()}@lido.local>"
        msg["Date"] = formatdate(self.clock(), usegmt=True)
        msg.set_content(f"A supervisor review is available.\nEvent identifier: {event_id}\nReceipt: {receipt}\n")
        try:
            with self.smtp_factory(self.config.host, self.config.port, timeout=self.config.timeout) as smtp:
                smtp.ehlo()
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
                smtp.login(self.config.username, self.config.password)
                refused = smtp.send_message(msg, from_addr=self.config.sender, to_addrs=list(self.config.recipients))
                if refused is None: refused = {}
                if not isinstance(refused, dict) or set(refused) or len(refused) != 0:
                    raise RuntimeError("SMTP delivery failed")
        except Exception:
            # Never include server, account, message, or credential data in errors/logs.
            raise RuntimeError("SMTP delivery failed") from None
