import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

_ws = re.compile(r"\s+")


# IDs -------------------------------------------------------------------------

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def hash5(text: str) -> str:
    """Hash curto de 5 digitos numericos (uso operacional/visual, nao para PK)."""
    value = int(sha1(text)[:12], 16) % 100000
    return f"{value:05d}"


def event_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    return sha1(url.strip())


def fighter_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    return sha1(url.strip())


def fight_id(event_id: str, red_url: str, blue_url: str, bout_order: str) -> str:
    base = "|".join([event_id, red_url, blue_url, bout_order])
    return sha1(base)


def short_id_from_id(value: str | None) -> str | None:
    if not value:
        return None
    return hash5(value.strip())


# IO --------------------------------------------------------------------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


# Parsing ---------------------------------------------------------------------

def clean(text):
    if text is None:
        return None
    t = _ws.sub(" ", text).strip()
    return t or None


def safe_select_one(parent, css: str):
    try:
        return parent.select_one(css)
    except Exception:
        return None


def safe_attr(tag, attr: str):
    if not tag:
        return None
    if tag.has_attr(attr):
        return str(tag.get(attr))
    return None


def parse_ufc_date(s) -> date | None:
    """UFC Stats usa: 'May 11, 2024'."""
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.strptime(s, "%B %d, %Y").date()
    except ValueError:
        return None
