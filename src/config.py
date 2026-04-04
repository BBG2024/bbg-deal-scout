"""Configuration loader for BBG Deal Scout."""

import os
import yaml
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_config = None
_config_path = None


def load_config(path: str = None) -> dict:
    """Load configuration from YAML file."""
    global _config, _config_path

    if path is None:
        # Look for config.yaml in project root
        root = Path(__file__).parent.parent
        path = root / "config.yaml"

    _config_path = Path(path)

    if not _config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {_config_path}\n"
            f"Copy config.yaml.example to config.yaml and fill in your credentials."
        )

    with open(_config_path, "r", encoding="utf-8") as f:
        _config = yaml.safe_load(f)

    # Override with environment variables where available
    _apply_env_overrides(_config)

    logger.info(f"Configuration loaded from {_config_path}")
    return _config


def _apply_env_overrides(cfg: dict):
    """Allow environment variables to override sensitive config values."""
    env_map = {
        "BBG_BING_API_KEY": ("bing_search", "api_key"),
        "BBG_EMAIL_PASSWORD": ("email_parsing", "email_password"),
        "BBG_SMTP_PASSWORD": ("notifications", "email", "sender_password"),
        "BBG_SLACK_WEBHOOK": ("notifications", "slack", "webhook_url"),
    }

    for env_var, key_path in env_map.items():
        val = os.environ.get(env_var)
        if val:
            obj = cfg
            for k in key_path[:-1]:
                obj = obj.get(k, {})
            obj[key_path[-1]] = val
            logger.debug(f"Config override applied from env: {env_var}")

    # Cloud deployment: dashboard user passwords via env vars
    admin_pw = os.environ.get("BBG_ADMIN_PASSWORD")
    if admin_pw:
        cfg.setdefault("dashboard", {}).setdefault("users", {})["admin"] = admin_pw

    principal2_pw = os.environ.get("BBG_PRINCIPAL2_PASSWORD")
    if principal2_pw:
        cfg.setdefault("dashboard", {}).setdefault("users", {})["principal2"] = principal2_pw

    # Cloud deployment: override database path (e.g. persistent volume mount)
    db_path = os.environ.get("BBG_DB_PATH")
    if db_path:
        cfg.setdefault("general", {})["database_path"] = db_path

    # Cloud deployment: override notification recipients via env var (comma-separated)
    recipients_env = os.environ.get("BBG_NOTIFY_RECIPIENTS")
    if recipients_env:
        recipients = [r.strip() for r in recipients_env.split(",") if r.strip()]
        cfg.setdefault("notifications", {}).setdefault("email", {})["recipients"] = recipients


def get_config() -> dict:
    """Get the loaded configuration. Loads from default path if not yet loaded."""
    global _config
    if _config is None:
        load_config()
    return _config


def get_regions() -> dict:
    """Get configured regions."""
    return get_config().get("regions", {})


def get_filters() -> dict:
    """Get listing filter criteria."""
    return get_config().get("filters", {})


def get_scoring() -> dict:
    """Get scoring thresholds."""
    return get_config().get("scoring", {})
