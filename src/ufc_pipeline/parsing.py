import re
from datetime import datetime, date
from bs4 import BeautifulSoup, Tag

_ws = re.compile(r"\s+")


def clean(text):
    if text is None:
        return None
    t = _ws.sub(" ", text).strip()
    return t or None


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


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
