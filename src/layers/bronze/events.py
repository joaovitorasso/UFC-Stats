import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

from utils.config import PipelineConfig
from utils.http_client import HttpClient
from utils.helpers import (
    clean,
    ensure_dir,
    event_id_from_url,
    parse_ufc_date,
    read_jsonl,
    safe_attr,
    safe_select_one,
    short_id_from_id,
    write_json,
    write_jsonl,
    write_text,
)

log = logging.getLogger(__name__)


def _parse_tabela_eventos(html: str, status: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.b-statistics__table-events")
    if not table:
        return []

    eventos = []
    for row in table.select("tbody tr.b-statistics__table-row"):
        tds = row.select("td")
        if len(tds) < 2:
            continue

        content = safe_select_one(tds[0], "i.b-statistics__table-content")
        if not content:
            continue

        a_tag = safe_select_one(content, "a.b-link")
        date_span = safe_select_one(content, "span.b-statistics__date")

        nome = clean(a_tag.get_text()) if a_tag else None
        link = safe_attr(a_tag, "href")
        raw_date = clean(date_span.get_text()) if date_span else None
        dt = parse_ufc_date(raw_date)
        date_iso = dt.isoformat() if dt else None
        local = clean(tds[1].get_text())
        eid = event_id_from_url(link)

        eventos.append(
            {
                "event_id": eid,
                "event_id_hash5": short_id_from_id(eid),
                "name": nome,
                "event_url": link,
                "event_date": date_iso,
                "location": local,
                "status": status,
            }
        )

    eventos.sort(key=lambda e: e.get("event_date") or "", reverse=True)
    return eventos


def _filtrar_incremental(eventos: list[dict], *, data_ancora: date, lookback_days: int) -> list[dict]:
    corte = data_ancora - timedelta(days=max(lookback_days, 0))
    resultado = []
    for ev in eventos:
        ev_date_str = ev.get("event_date")
        ev_date = date.fromisoformat(ev_date_str) if ev_date_str else None
        status = (ev.get("status") or "").lower()

        if status == "upcoming":
            resultado.append(ev)
        elif status == "completed":
            if ev_date and ev_date >= corte:
                resultado.append(ev)
        elif ev_date and ev_date >= corte:
            resultado.append(ev)
    return resultado


def coletar_index(cfg: PipelineConfig, sources: dict, *, dt: str, run_id: str, full_load: bool = False) -> Path:
    client = HttpClient(cfg.http)
    ufc = sources["ufcstats"]
    data_ancora = date.fromisoformat(dt)

    html_concluidos = client.get_text(ufc["completed_events_url"])
    html_proximos = client.get_text(ufc["upcoming_events_url"])
    eventos = _parse_tabela_eventos(html_concluidos, "completed") + _parse_tabela_eventos(html_proximos, "upcoming")

    if full_load:
        log.info("[RAW] Carga inicial (full load): sem filtro de lookback, %d eventos coletados.", len(eventos))
    elif cfg.events_lookback_days > 0:
        eventos = _filtrar_incremental(eventos, data_ancora=data_ancora, lookback_days=cfg.events_lookback_days)
        log.info("[RAW] Carga incremental: lookback=%d dias, %d eventos apos filtro.", cfg.events_lookback_days, len(eventos))

    if cfg.limit_events and cfg.limit_events > 0:
        eventos = eventos[: cfg.limit_events]

    out_dir = cfg.data_dir / "raw" / "indice_eventos" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "indice_eventos.jsonl"
    write_jsonl(out_path, eventos)

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}.json"
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "stage": "raw_events_index",
            "rows": len(eventos),
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )
    return out_path


# Alias para compatibilidade com o código legado
coletar_eventos_index = coletar_index


def baixar_html(cfg: PipelineConfig, events_index_path: Path, *, dt: str, run_id: str, use_cache: bool = True) -> Path:
    client = HttpClient(cfg.http)
    eventos = read_jsonl(events_index_path)

    out_dir = cfg.data_dir / "raw" / "html" / "eventos" / f"dt={dt}"
    ensure_dir(out_dir)

    workers = cfg.http.workers
    total = len(eventos)
    cache_hits = sum(
        1
        for ev in eventos
        if use_cache and (out_dir / f"{ev.get('event_id') or event_id_from_url(ev.get('event_url'))}.html").exists()
    )
    msg = f"[RAW/html] {total} eventos ({cache_hits} em cache, {total - cache_hits} a baixar) - {workers} workers"
    print(msg, flush=True)
    log.info(msg)

    def _download_evento(args: tuple[int, dict]) -> bool:
        i, ev = args
        url = ev.get("event_url")
        eid = ev.get("event_id") or event_id_from_url(url)
        if not url or not eid:
            return False

        html_path = out_dir / f"{eid}.html"
        if use_cache and html_path.exists():
            return True

        print(f"  [{i}/{total}] {ev.get('name') or eid}", flush=True)
        html = client.get_text(url)
        write_text(html_path, html)
        return False

    count = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_download_evento, (i, ev)): ev for i, ev in enumerate(eventos, start=1)}
        for future in as_completed(futures):
            try:
                from_cache = future.result()
                if from_cache:
                    skipped += 1
                else:
                    count += 1
            except Exception as exc:
                ev = futures[future]
                log.warning("[RAW/html] Erro em %s: %s", ev.get("name"), exc)

    log.info("[RAW/html] %d HTMLs de eventos baixados, %d ja existiam (pulados).", count, skipped)

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}_event_html.json"
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "stage": "raw_event_html",
            "rows": count,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )
    return out_dir


# Alias para compatibilidade com o código legado
baixar_html_eventos = baixar_html


def gerar_bronze(cfg: PipelineConfig, events_index_path: Path, *, dt: str, run_id: str) -> Path:
    eventos = read_jsonl(events_index_path)
    ingested_at = datetime.utcnow().isoformat() + "Z"
    for e in eventos:
        e["ingested_at"] = ingested_at

    out_dir = cfg.data_dir / "bronze" / "eventos" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "eventos.jsonl"
    write_jsonl(out_path, eventos)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_events_meta.json"
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "table": "bronze_eventos",
            "rows": len(eventos),
            "created_at": ingested_at,
        },
    )
    return out_path


# Alias para compatibilidade com o código legado
gerar_bronze_eventos = gerar_bronze
