"""Helpers for EVENT_CONFIGS__{region}__* environment flags."""

import os


def is_only_spectator_report(region: str) -> bool:
    """True when this region uses spectator-only reporting (skip full ticket Slack, etc.)."""
    for env_key in (
        f"EVENT_CONFIGS__{region}__only_specator_report",
        f"EVENT_CONFIGS__{region}__only_spectator_report",
    ):
        if os.getenv(env_key, "false").strip().lower() in ("true", "1"):
            return True
    return False


def any_only_spectator_report() -> bool:
    """True if any region has spectator-only reporting enabled."""
    for key in os.environ:
        if not key.startswith("EVENT_CONFIGS__"):
            continue
        if key.endswith("__only_specator_report") or key.endswith("__only_spectator_report"):
            if os.getenv(key, "false").strip().lower() in ("true", "1"):
                return True
    return False
