import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from ufc_pipeline.config import PipelineConfig
from ufc_pipeline.http import HttpClient
from ufc_pipeline.ids import fighter_id_from_url, short_id_from_id
from ufc_pipeline.io import ensure_dir, read_jsonl, read_text, write_json, write_jsonl, write_text
from ufc_pipeline.parsing import clean

log = logging.getLogger(__name__)


# ── parsing ──────────────────────────────────────────────────────────────────

def _primeiro_texto_p(td) -> str | None:
    ps = td.select("p.b-fight-details__table-text")
    return clean(ps[0].get_text()) if ps else None


def _parse_career_stats(soup: BeautifulSoup) -> dict:
    label_map = {
        "SLPM": "slpm",
        "STR. ACC.": "str_acc_pct",
        "SAPM": "sapm",
        "STR. DEF": "str_def_pct",
        "STR. DEF.": "str_def_pct",
        "TD AVG.": "td_avg_15min",
        "TD ACC.": "td_acc_pct",
        "TD DEF.": "td_def_pct",
        "SUB. AVG.": "sub_avg_15min",
    }
    stats = {
        "slpm": None,
        "str_acc_pct": None,
        "sapm": None,
        "str_def_pct": None,
        "td_avg_15min": None,
        "td_acc_pct": None,
        "td_def_pct": None,
        "sub_avg_15min": None,
    }
    for li in soup.select("li.b-list__box-list-item"):
        parts = list(li.stripped_strings)
        if not parts:
            continue
        raw_label = clean(parts[0]).rstrip(":").upper()
        key = label_map.get(raw_label)
        if not key:
            continue
        value = clean(" ".join(parts[1:])) if len(parts) > 1 else None
        stats[key] = value
    return stats


def _parse_pagina_lutador(html: str, profile_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    name_tag = soup.select_one("span.b-content__title-highlight")
    nome = clean(name_tag.get_text()) if name_tag else None

    record_span = soup.select_one("span.b-content__title-record")
    cartel = None
    if record_span:
        record_text = clean(record_span.get_text())
        if record_text and record_text.lower().startswith("record:"):
            cartel = record_text.split(":", 1)[1].strip()

    altura = peso = alcance = stance = dob = None
    bio_ul = soup.select_one("ul.b-list__box-list")
    if bio_ul:
        for li in bio_ul.select("li.b-list__box-list-item"):
            parts = list(li.stripped_strings)
            if not parts:
                continue
            label = clean(parts[0]).rstrip(":").upper() if parts[0] else ""
            valor = clean(" ".join(parts[1:])) if len(parts) > 1 else None
            if label == "HEIGHT":
                altura = valor
            elif label == "WEIGHT":
                peso = valor
            elif label == "REACH":
                alcance = valor
            elif label == "STANCE":
                stance = valor
            elif label == "DOB":
                dob = valor

    career_stats = _parse_career_stats(soup)

    historico: list[dict] = []
    table = soup.select_one("table.b-fight-details__table.b-fight-details__table_type_event-details")
    if table:
        tbody = table.select_one("tbody")
        if tbody:
            for row in tbody.select("tr.b-fight-details__table-row"):
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue

                resultado = None
                flag = cols[0].select_one("a.b-flag .b-flag__text")
                if flag:
                    resultado = clean(flag.get_text())

                fighter_col = cols[1]
                fighter_ps = fighter_col.select("p.b-fight-details__table-text a.b-link")
                fighter_name = clean(fighter_ps[0].get_text()) if len(fighter_ps) > 0 else None
                opponent_name = clean(fighter_ps[1].get_text()) if len(fighter_ps) > 1 else None
                fighter_as = fighter_col.select('a[href*="fighter-details/"]')
                fighter_link = fighter_as[0].get("href") if len(fighter_as) > 0 else None
                opponent_link = fighter_as[1].get("href") if len(fighter_as) > 1 else None

                event_col = cols[6]
                event_a = event_col.select_one("a.b-link")
                event_name = clean(event_a.get_text()) if event_a else None
                event_link = event_a.get("href") if event_a and event_a.has_attr("href") else None
                event_ps = event_col.select("p.b-fight-details__table-text")
                event_date = None
                title_bout = False
                if event_ps:
                    event_date_p = event_ps[-1]
                    if event_date_p.find("img", src=lambda s: s and "belt.png" in s):
                        title_bout = True
                    event_date = clean(event_date_p.get_text())

                method_col = cols[7]
                method_ps = method_col.select("p.b-fight-details__table-text")
                method_short = clean(method_ps[0].get_text()) if len(method_ps) > 0 else None
                method_detail = clean(method_ps[1].get_text()) if len(method_ps) > 1 else None

                fight_link = row.get("data-link")
                if not fight_link:
                    flag_a = cols[0].select_one("a.b-flag")
                    if flag_a and flag_a.has_attr("href"):
                        fight_link = flag_a["href"]

                historico.append({
                    "result": resultado,
                    "fight_url": fight_link,
                    "fighter": fighter_name,
                    "fighter_profile_url": fighter_link,
                    "opponent": opponent_name,
                    "opponent_profile_url": opponent_link,
                    "kd": _primeiro_texto_p(cols[2]),
                    "str": _primeiro_texto_p(cols[3]),
                    "td": _primeiro_texto_p(cols[4]),
                    "sub": _primeiro_texto_p(cols[5]),
                    "event_name": event_name,
                    "event_url": event_link,
                    "event_date": event_date,
                    "title_bout": title_bout,
                    "method_short": method_short,
                    "method_detail": method_detail,
                    "round": clean(cols[8].get_text()),
                    "time": clean(cols[9].get_text()),
                })

    fid = fighter_id_from_url(profile_url)
    return {
        "fighter_id": fid,
        "fighter_id_hash5": short_id_from_id(fid),
        "name": nome,
        "profile_url": profile_url,
        "bio": {
            "record": cartel,
            "height": altura,
            "weight": peso,
            "reach": alcance,
            "stance": stance,
            "dob": dob,
        },
        "career_stats": career_stats,
        "fights": historico,
    }


# ── etapas de pipeline ────────────────────────────────────────────────────────

def baixar_html(cfg: PipelineConfig, fighters_index_path: Path, *, dt: str, run_id: str) -> Path:
    client = HttpClient(cfg.http)
    lutadores = read_jsonl(fighters_index_path)

    if cfg.limit_fighters and cfg.limit_fighters > 0:
        lutadores = lutadores[:cfg.limit_fighters]

    out_dir = cfg.data_dir / "raw" / "html" / "lutadores" / f"dt={dt}"
    ensure_dir(out_dir)

    # Conjunto de fighter_ids já baixados em QUALQUER run anterior (dt=* diferente do atual).
    # Evita re-baixar perfis de lutadores que não lutaram no período atual.
    html_root = cfg.data_dir / "raw" / "html" / "lutadores"
    ids_em_cache_anterior: set[str] = {
        p.stem
        for d in html_root.iterdir()
        if d.is_dir() and d != out_dir
        for p in d.glob("*.html")
    }

    workers = cfg.http.workers
    total = len(lutadores)
    cache_hits = sum(
        1 for f in lutadores
        if (out_dir / f"{f.get('fighter_id') or fighter_id_from_url(f.get('profile_url'))}.html").exists()
        or (f.get("fighter_id") or fighter_id_from_url(f.get("profile_url") or "")) in ids_em_cache_anterior
    )
    msg = f"[RAW/lutadores] {total} lutadores ({cache_hits} em cache, {total - cache_hits} a baixar) — {workers} workers"
    print(msg, flush=True)
    log.info(msg)

    def _download_lutador(args: tuple[int, dict]) -> tuple[str, bool]:
        i, f = args
        url = f.get("profile_url")
        fid = f.get("fighter_id") or fighter_id_from_url(url)
        if not url or not fid:
            return fid or "", False
        html_path = out_dir / f"{fid}.html"
        if html_path.exists():
            return fid, True  # cache na pasta atual
        if fid in ids_em_cache_anterior:
            return fid, True  # já existe em partição anterior — não re-baixar
        print(f"  [{i}/{total}] {f.get('name') or fid}", flush=True)
        html = client.get_text(url)
        write_text(html_path, html)
        return fid, False

    count = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_download_lutador, (i, f)): f for i, f in enumerate(lutadores, start=1)}
        concluidos = 0
        for future in as_completed(futures):
            concluidos += 1
            try:
                _, from_cache = future.result()
                if from_cache:
                    skipped += 1
                else:
                    count += 1
            except Exception as exc:
                f = futures[future]
                log.warning("[RAW/lutadores] Erro em %s: %s", f.get("name"), exc)
            if concluidos % 100 == 0 or concluidos == total:
                print(f"  [progresso] {concluidos}/{total} lutadores processados", flush=True)

    log.info("[RAW/lutadores] %d baixados, %d já existiam (pulados).", count, skipped)

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}_fighter_html.json"
    write_json(meta_path, {
        "run_id": run_id, "dt": dt, "stage": "raw_fighter_html",
        "rows": count, "created_at": datetime.utcnow().isoformat() + "Z",
    })
    return out_dir


def gerar_bronze(
    cfg: PipelineConfig,
    fighters_index_path: Path,
    fighter_html_dir: Path,
    *,
    dt: str,
    run_id: str,
) -> Path:
    index = read_jsonl(fighters_index_path)
    if cfg.limit_fighters and cfg.limit_fighters > 0:
        index = index[:cfg.limit_fighters]

    # Monta índice de todos os HTMLs disponíveis nas partições anteriores
    # (para encontrar perfis baixados em runs passados que não foram re-baixados hoje)
    html_root = fighter_html_dir.parent
    html_por_id: dict[str, Path] = {}
    for dt_dir in sorted(html_root.iterdir()):
        if not dt_dir.is_dir():
            continue
        for p in dt_dir.glob("*.html"):
            html_por_id[p.stem] = p  # partições mais recentes sobrescrevem as antigas

    rows: list[dict] = []
    for lutador in index:
        fighter_id = lutador.get("fighter_id")
        profile_url = lutador.get("profile_url")
        if not fighter_id or not profile_url:
            continue
        html_path = html_por_id.get(fighter_id)
        if html_path is None or not html_path.exists():
            continue
        rows.append(_parse_pagina_lutador(read_text(html_path), profile_url=profile_url))

    ingested_at = datetime.utcnow().isoformat() + "Z"
    for row in rows:
        row["ingested_at"] = ingested_at

    out_dir = cfg.data_dir / "bronze" / "lutadores" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "lutadores.jsonl"
    write_jsonl(out_path, rows)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_fighters_meta.json"
    write_json(meta_path, {
        "run_id": run_id, "dt": dt, "table": "bronze_lutadores",
        "rows": len(rows), "created_at": ingested_at,
    })
    return out_path
