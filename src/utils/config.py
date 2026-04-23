import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from utils.http_client import HttpConfig


def _expand_env(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    m = re.findall(r"\$\{([A-Z0-9_]+)(:-([^}]*))?\}", value)
    out = value
    for var, _, default in m:
        rep = os.getenv(var, default or "")
        out = out.replace(f"${{{var}:-{default}}}", rep)
        out = out.replace(f"${{{var}}}", rep)
    return out


def _walk(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _walk(_expand_env(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(_expand_env(v)) for v in obj]
    return _expand_env(obj)


@dataclass(frozen=True)
class PipelineConfig:
    data_dir: Path
    http: HttpConfig
    events_lookback_days: int
    limit_events: int
    limit_fights_per_event: int
    limit_fighters: int


def load_config(repo_root: Path) -> tuple[PipelineConfig, Dict[str, Any]]:
    load_dotenv(repo_root / ".env", override=False)

    settings_path = repo_root / "configs" / "settings.yaml"
    sources_path = repo_root / "configs" / "sources.yaml"

    settings = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    sources = yaml.safe_load(sources_path.read_text(encoding="utf-8"))

    settings = _walk(settings)
    sources = _walk(sources)

    data_dir = Path(settings["data_dir"]).resolve()

    http_cfg = settings.get("http", {})
    http = HttpConfig(
        user_agent=str(http_cfg.get("user_agent", "Mozilla/5.0 (ufc-lakehouse)")),
        timeout_seconds=int(http_cfg.get("timeout_seconds", 20)),
        retries=int(http_cfg.get("retries", 4)),
        backoff_factor=float(http_cfg.get("backoff_factor", 0.6)),
        polite_delay_seconds=float(http_cfg.get("polite_delay_seconds", 0.6)),
        max_requests_per_minute=int(http_cfg.get("max_requests_per_minute", 80)),
        workers=int(http_cfg.get("workers", 4)),
    )

    pipe_cfg = settings.get("pipeline", {})
    cfg = PipelineConfig(
        data_dir=data_dir,
        http=http,
        events_lookback_days=int(pipe_cfg.get("events_lookback_days", 30)),
        limit_events=int(pipe_cfg.get("limit_events", 0)),
        limit_fights_per_event=int(pipe_cfg.get("limit_fights_per_event", 0)),
        limit_fighters=int(pipe_cfg.get("limit_fighters", 0)),
    )
    return cfg, sources
