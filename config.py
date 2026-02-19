import os
import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    TRUENAS_URL: Optional[str]
    TRUENAS_API_KEY: Optional[str]
    TRUENAS_WS_URL_OVERRIDE: Optional[str] = None
    TRUENAS_WS_PATH: str = "/websocket"
    TRUENAS_VERIFY_TLS: bool = False

    FLASK_SECRET_KEY: str = "change-me"

    # RBAC: bcrypt hashed passwords (not plaintext)
    ADMIN_PASSWORD_HASH: str = ""
    VIEWER_PASSWORD_HASH: str = ""

    AUDIT_LOG_PATH: str = "/tmp/shadowportal_audit.jsonl"

    @property
    def TRUENAS_WS_URL(self) -> str:
        if self.TRUENAS_WS_URL_OVERRIDE:
            return self.TRUENAS_WS_URL_OVERRIDE

        if not self.TRUENAS_URL:
            raise ValueError("TRUENAS_URL is not configured")

        base = self.TRUENAS_URL.strip().rstrip("/")
        p = urlparse(base)

        if not p.scheme or not p.netloc:
            raise ValueError(
                f"TRUENAS_URL must include scheme and host; got: {self.TRUENAS_URL!r}"
            )

        if p.scheme not in {"http", "https"}:
            raise ValueError(
                f"TRUENAS_URL scheme must be http or https; got: {p.scheme!r}"
            )

        ws_scheme = "wss" if p.scheme == "https" else "ws"
        return urlunparse((ws_scheme, p.netloc, self.TRUENAS_WS_PATH, "", "", ""))


def load_settings() -> Settings:
    url = _env("TRUENAS_URL")
    api_key = os.getenv("TRUENAS_API_KEY", "").strip()
    ws_url = _env("TRUENAS_WS_URL")
    ws_path = _env("TRUENAS_WS_PATH", "/websocket") or "/websocket"
    verify_tls = _env_bool("TRUENAS_VERIFY_TLS", default=False)

    secret = _env("FLASK_SECRET_KEY", "change-me") or "change-me"

    admin_hash = _env("SHADOWPORTAL_ADMIN_PASSWORD_HASH", "") or ""
    viewer_hash = _env("SHADOWPORTAL_VIEWER_PASSWORD_HASH", "") or ""

    audit_path = _env("SHADOWPORTAL_AUDIT_LOG", "/tmp/shadowportal_audit.jsonl") or "/tmp/shadowportal_audit.jsonl"

    if not url:
        logger.warning("TRUENAS_URL not set; app running in unconfigured mode")
    if not api_key:
        logger.warning("TRUENAS_API_KEY not set; app running in unconfigured mode")

    return Settings(
        TRUENAS_URL=url,
        TRUENAS_API_KEY=api_key,
        TRUENAS_WS_URL_OVERRIDE=ws_url,
        TRUENAS_WS_PATH=ws_path,
        TRUENAS_VERIFY_TLS=verify_tls,
        FLASK_SECRET_KEY=secret,
        ADMIN_PASSWORD_HASH=admin_hash,
        VIEWER_PASSWORD_HASH=viewer_hash,
        AUDIT_LOG_PATH=audit_path,
    )


settings = load_settings()


def is_configured() -> bool:
    return bool(settings.TRUENAS_URL and settings.TRUENAS_API_KEY)
