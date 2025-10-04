# src/com/lingenhag/rrp/platform/config/settings.py
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    config: Dict[str, Any]

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "Settings":
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logging.warning(f"Konfigurationsdatei {config_path} nicht gefunden. Verwende Defaults.")
            config = {}
        return cls(config=config)

    def get(self, section: str, key: str, default: Any = None) -> Any:
        return self.config.get(section, {}).get(key, default)

    def get_api_key(self, key_name: str, section: str) -> str | None:
        return self.config.get(section, {}).get("api_key") or os.getenv(key_name)