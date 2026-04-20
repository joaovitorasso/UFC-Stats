import hashlib


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
