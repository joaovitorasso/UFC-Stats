import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from utils.config import PipelineConfig
from utils.http_client import HttpClient
from utils.helpers import (
    clean,
    ensure_dir,
    fight_id,
    fighter_id_from_url,
    read_jsonl,
    read_text,
    safe_attr,
    short_id_from_id,
    write_json,
    write_jsonl,
    write_text,
)

log = logging.getLogger(__name__)


def _codigo_bonus(row) -> int | None:
    srcs = [str(img.get("src") or "").lower() for img in row.select("img")]
    if any("fight.png" in s for s in srcs):
        return 1
    if any("perf.png" in s for s in srcs):
        return 2
    if any("sub.png" in s for s in srcs):
        return 3
    if any("ko.png" in s for s in srcs):
        return 4
    return None


def _tipo_bout(row) -> str:
    srcs = [str(img.get("src") or "").lower() for img in row.select("img")]
    return "title bout" if any("belt.png" in s for s in srcs) else "normal"


def _parse_links_lutas(event_html: str) -> tuple[str | None, list[dict]]:
    soup = BeautifulSoup(event_html, "html.parser")
    name_tag = soup.select_one("h2.b-content__title span")
    event_name = clean(name_tag.get_text()) if name_tag else None
    lutas = []
    for row in soup.select("tr.b-fight-details__table-row"):
        link = row.get("data-link")
        if not link:
            continue
        lutas.append(
            {
                "fight_url": str(link),
                "bonus_code": _codigo_bonus(row),
                "bout": _tipo_bout(row),
            }
        )
    return event_name, lutas


def _parse_cabecalho_lutadores(soup: BeautifulSoup) -> list[dict]:
    lutadores = []
    for f in soup.select(".b-fight-details__person"):
        name_tag = f.select_one(".b-fight-details__person-name a")
        result_tag = f.select_one(".b-fight-details__person-status")
        link_tag = f.select_one(".b-fight-details__person-link")
        link = safe_attr(link_tag, "href")
        fid = fighter_id_from_url(link)
        lutadores.append(
            {
                "name": clean(name_tag.get_text()) if name_tag else None,
                "profile_url": link,
                "fighter_id": fid,
                "fighter_id_hash5": short_id_from_id(fid),
                "result": clean(result_tag.get_text()) if result_tag else None,
            }
        )
    return lutadores


def _parse_meta_luta(soup: BeautifulSoup) -> dict:
    meta = {"method": None, "round": None, "time": None, "time_format": None, "referee": None}
    blocos = soup.select(
        ".b-fight-details__text .b-fight-details__text-item, "
        ".b-fight-details__text .b-fight-details__text-item_first"
    )
    for item in blocos:
        label_tag = item.select_one(".b-fight-details__label")
        if not label_tag:
            continue
        label = label_tag.get_text(strip=True)
        valor = item.get_text(" ", strip=True).replace(label, "").strip()
        if label == "Method:":
            meta["method"] = clean(valor)
        elif label == "Round:":
            meta["round"] = clean(valor)
        elif label == "Time:":
            meta["time"] = clean(valor)
        elif label == "Time format:":
            meta["time_format"] = clean(valor)
        elif label == "Referee:":
            meta["referee"] = clean(valor)
    return meta


def _parse_golpes_por_round(soup: BeautifulSoup) -> dict:
    sig_title_p = soup.find(
        "p",
        class_="b-fight-details__collapse-link_tot",
        string=lambda t: t and "Significant Strikes" in t,
    )
    if not sig_title_p:
        return {}

    sig_section = sig_title_p.find_parent("section", class_="b-fight-details__section js-fight-section")
    if not sig_section:
        return {}

    sig_per_round = sig_section.find_next_sibling("section", class_="b-fight-details__section js-fight-section")
    if not sig_per_round:
        return {}

    tabela = sig_per_round.select_one("table.b-fight-details__table.js-fight-table")
    if not tabela:
        return {}

    tbody = tabela.find("tbody")
    if not tbody:
        return {}

    round_atual = None
    tmp: dict = {}
    labels = ["sig_str", "sig_str_pct", "head", "body", "leg", "distance", "clinch", "ground"]

    for child in tbody.children:
        nome = getattr(child, "name", None)
        if nome == "thead":
            txt = child.get_text(strip=True)
            if "Round" in txt:
                round_atual = txt.split()[-1]
        elif nome == "tr" and round_atual:
            cols = child.find_all("td")
            if len(cols) < 9:
                continue
            links = cols[0].select("a.b-link.b-link_style_black")
            f1 = clean(links[0].get_text()) if len(links) > 0 else None
            f2 = clean(links[1].get_text()) if len(links) > 1 else None

            def parse_row(idx: int) -> dict:
                out = {}
                for label, col in zip(labels, cols[1:]):
                    ps = col.select("p.b-fight-details__table-text")
                    vals = [clean(p.get_text()) for p in ps]
                    if len(vals) > idx:
                        out[label] = vals[idx]
                return out

            if f1:
                tmp.setdefault(f1, {})[round_atual] = parse_row(0)
            if f2:
                tmp.setdefault(f2, {})[round_atual] = parse_row(1)
    return tmp


def _parse_pagina_luta(
    fight_html: str,
    *,
    event_id: str,
    event_name: str | None,
    fight_url: str,
    bout_order: str,
    bonus_code: int | None = None,
    bout: str = "normal",
) -> dict:
    soup = BeautifulSoup(fight_html, "html.parser")
    lutadores = _parse_cabecalho_lutadores(soup)
    meta = _parse_meta_luta(soup)
    golpes = _parse_golpes_por_round(soup)

    for f in lutadores:
        nome = f.get("name")
        f["rounds_sig_strikes"] = golpes.get(nome, {}) if nome else {}

    red_url = lutadores[0].get("profile_url") if len(lutadores) > 0 else ""
    blue_url = lutadores[1].get("profile_url") if len(lutadores) > 1 else ""
    fid = fight_id(event_id, red_url or "", blue_url or "", bout_order)

    return {
        "fight_id": fid,
        "fight_id_hash5": short_id_from_id(fid),
        "event_id": event_id,
        "event_id_hash5": short_id_from_id(event_id),
        "event_name": event_name,
        "fight_url": fight_url,
        "bout_order": bout_order,
        "bonus_code": bonus_code,
        "bout": bout,
        **meta,
        "fighters": lutadores,
    }


def _download_fight_html(client: HttpClient, fight_url: str, fight_cache: Path, label: str, *, use_cache: bool = True) -> tuple[str, bool]:
    if use_cache and fight_cache.exists():
        return read_text(fight_cache), True

    print(f"  {label} GET: {fight_url}", flush=True)
    html = client.get_text(fight_url)
    write_text(fight_cache, html)
    return html, False


def gerar_bronze(
    cfg: PipelineConfig,
    events_index_path: Path,
    event_html_dir: Path,
    *,
    dt: str,
    run_id: str,
    use_cache: bool = True,
) -> tuple[Path, Path, Path]:
    client = HttpClient(cfg.http)
    workers = cfg.http.workers
    eventos = read_jsonl(events_index_path)

    fight_html_dir = cfg.data_dir / "raw" / "html" / "lutas" / f"dt={dt}"
    ensure_dir(fight_html_dir)

    total_eventos = len(eventos)
    log.info("[BRONZE/lutas] Fase 1: coletando URLs de %d eventos...", total_eventos)

    tarefas: list[dict] = []
    for ev_idx, ev in enumerate(eventos, start=1):
        eid = ev.get("event_id")
        if not eid:
            continue

        html_path = event_html_dir / f"{eid}.html"
        if not html_path.exists():
            log.warning("[BRONZE/lutas] HTML nao encontrado, pulando evento %s", eid)
            continue

        event_html = read_text(html_path)
        event_name, fight_rows = _parse_links_lutas(event_html)
        if cfg.limit_fights_per_event and cfg.limit_fights_per_event > 0:
            fight_rows = fight_rows[: cfg.limit_fights_per_event]

        event_status = ev.get("status") or "completed"

        for idx, row in enumerate(fight_rows, start=1):
            fight_url = str(row.get("fight_url") or "")
            if not fight_url:
                continue
            fight_slug = fight_url.rstrip("/").split("/")[-1]
            fight_cache = fight_html_dir / f"{fight_slug}.html"
            tarefas.append(
                {
                    "fight_url": fight_url,
                    "fight_cache": fight_cache,
                    "was_cached": use_cache and fight_cache.exists(),
                    "label": f"[luta {idx}/{len(fight_rows)} | evento {ev_idx}/{total_eventos}]",
                    "event_id": eid,
                    "event_name": event_name,
                    "event_status": event_status,
                    "bout_order": str(idx),
                    "bonus_code": row.get("bonus_code"),
                    "bout": str(row.get("bout") or "normal"),
                }
            )

    total_lutas = len(tarefas)
    cache_hits = sum(1 for t in tarefas if use_cache and t["fight_cache"].exists())
    msg = f"[BRONZE/lutas] {total_lutas} lutas ({cache_hits} em cache, {total_lutas - cache_hits} a baixar) - {workers} workers"
    print(msg, flush=True)
    log.info(msg)

    htmls: dict[str, str] = {}
    a_baixar_lutas = total_lutas - cache_hits

    def _worker(tarefa: dict) -> tuple[str, str, bool]:
        html, from_cache = _download_fight_html(client, tarefa["fight_url"], tarefa["fight_cache"], tarefa["label"], use_cache=use_cache)
        return tarefa["fight_url"], html, from_cache

    baixados_lutas = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tarefas}
        for future in as_completed(futures):
            try:
                url, html, from_cache = future.result()
                htmls[url] = html
                if not from_cache:
                    baixados_lutas += 1
            except Exception as exc:
                t = futures[future]
                msg_err = f"  {t['label']} ERRO: {exc}"
                print(msg_err, flush=True)
                log.warning(msg_err)
            if a_baixar_lutas > 0 and (baixados_lutas % 50 == 0 or baixados_lutas == a_baixar_lutas):
                print(f"  [progresso] {baixados_lutas}/{a_baixar_lutas} lutas baixadas", flush=True)

    log.info("[BRONZE/lutas] Fase 3: parseando %d HTMLs...", len(htmls))
    todas_lutas: list[dict] = []
    index_lutadores: dict[str, dict] = {}

    for t in tarefas:
        fight_html = htmls.get(t["fight_url"])
        if not fight_html:
            continue

        luta = _parse_pagina_luta(
            fight_html,
            event_id=t["event_id"],
            event_name=t["event_name"],
            fight_url=t["fight_url"],
            bout_order=t["bout_order"],
            bonus_code=t["bonus_code"],
            bout=t["bout"],
        )
        luta["event_status"] = t["event_status"]

        for person in luta.get("fighters") or []:
            fid = person.get("fighter_id")
            url = person.get("profile_url")
            if url and fid and fid not in index_lutadores:
                index_lutadores[fid] = {
                    "fighter_id": fid,
                    "fighter_id_hash5": person.get("fighter_id_hash5"),
                    "name": person.get("name"),
                    "profile_url": url,
                }
        todas_lutas.append(luta)

    log.info("[BRONZE/lutas] Total: %d lutas parseadas, %d lutadores unicos", len(todas_lutas), len(index_lutadores))

    ingested_at = datetime.utcnow().isoformat() + "Z"
    for r in todas_lutas:
        r["ingested_at"] = ingested_at

    log.info("[BRONZE/lutas] %d lutas parseadas, %d lutadores unicos.", len(todas_lutas), len(index_lutadores))

    out_dir = cfg.data_dir / "bronze" / "lutas" / f"dt={dt}"
    ensure_dir(out_dir)
    lutas_path = out_dir / "lutas.jsonl"
    write_jsonl(lutas_path, todas_lutas)

    index_dir = cfg.data_dir / "bronze" / "lutadores" / f"dt={dt}"
    ensure_dir(index_dir)
    index_path = index_dir / "lutadores_index.jsonl"
    lista = list(index_lutadores.values())
    for r in lista:
        r["ingested_at"] = ingested_at
    write_jsonl(index_path, lista)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_fights_meta.json"
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "table": "bronze_lutas",
            "rows": len(todas_lutas),
            "created_at": ingested_at,
        },
    )
    return lutas_path, index_path


# Alias para compatibilidade com o código legado
gerar_bronze_lutas = gerar_bronze
