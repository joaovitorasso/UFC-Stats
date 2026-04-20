import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

from config import PipelineConfig
from http_client import HttpClient
from utils import (
    clean,
    ensure_dir,
    event_id_from_url,
    fight_id,
    fighter_id_from_url,
    parse_ufc_date,
    read_jsonl,
    read_text,
    safe_attr,
    safe_select_one,
    short_id_from_id,
    write_json,
    write_jsonl,
    write_text,
)

log = logging.getLogger(__name__)


# Eventos ---------------------------------------------------------------------

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


def coletar_eventos_index(cfg: PipelineConfig, sources: dict, *, dt: str, run_id: str) -> Path:
    client = HttpClient(cfg.http)
    ufc = sources["ufcstats"]
    data_ancora = date.fromisoformat(dt)

    html_concluidos = client.get_text(ufc["completed_events_url"])
    html_proximos = client.get_text(ufc["upcoming_events_url"])
    eventos = _parse_tabela_eventos(html_concluidos, "completed") + _parse_tabela_eventos(html_proximos, "upcoming")

    if cfg.events_lookback_days > 0:
        eventos = _filtrar_incremental(eventos, data_ancora=data_ancora, lookback_days=cfg.events_lookback_days)

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


def baixar_html_eventos(cfg: PipelineConfig, events_index_path: Path, *, dt: str, run_id: str) -> Path:
    client = HttpClient(cfg.http)
    eventos = read_jsonl(events_index_path)

    out_dir = cfg.data_dir / "raw" / "html" / "eventos" / f"dt={dt}"
    ensure_dir(out_dir)

    workers = cfg.http.workers
    total = len(eventos)
    cache_hits = sum(
        1
        for ev in eventos
        if (out_dir / f"{ev.get('event_id') or event_id_from_url(ev.get('event_url'))}.html").exists()
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
        if html_path.exists():
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


def gerar_bronze_eventos(cfg: PipelineConfig, events_index_path: Path, *, dt: str, run_id: str) -> Path:
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


# Lutas -----------------------------------------------------------------------

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


def _download_fight_html(client: HttpClient, fight_url: str, fight_cache: Path, label: str) -> tuple[str, bool]:
    if fight_cache.exists():
        print(f"  {label} cache", flush=True)
        return read_text(fight_cache), True

    print(f"  {label} GET: {fight_url}", flush=True)
    html = client.get_text(fight_url)
    write_text(fight_cache, html)
    return html, False


def gerar_bronze_lutas(
    cfg: PipelineConfig,
    events_index_path: Path,
    event_html_dir: Path,
    *,
    dt: str,
    run_id: str,
) -> tuple[Path, Path]:
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
                    "was_cached": fight_cache.exists(),
                    "label": f"[luta {idx}/{len(fight_rows)} | evento {ev_idx}/{total_eventos}]",
                    "event_id": eid,
                    "event_name": event_name,
                    "bout_order": str(idx),
                    "bonus_code": row.get("bonus_code"),
                    "bout": str(row.get("bout") or "normal"),
                }
            )

    total_lutas = len(tarefas)
    cache_hits = sum(1 for t in tarefas if t["fight_cache"].exists())
    msg = f"[BRONZE/lutas] {total_lutas} lutas ({cache_hits} em cache, {total_lutas - cache_hits} a baixar) - {workers} workers"
    print(msg, flush=True)
    log.info(msg)

    htmls: dict[str, str] = {}

    def _worker(tarefa: dict) -> tuple[str, str]:
        html, _ = _download_fight_html(client, tarefa["fight_url"], tarefa["fight_cache"], tarefa["label"])
        return tarefa["fight_url"], html

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tarefas}
        concluidos = 0
        for future in as_completed(futures):
            concluidos += 1
            try:
                url, html = future.result()
                htmls[url] = html
            except Exception as exc:
                t = futures[future]
                msg_err = f"  {t['label']} ERRO: {exc}"
                print(msg_err, flush=True)
                log.warning(msg_err)
            if concluidos % 50 == 0 or concluidos == total_lutas:
                print(f"  [progresso] {concluidos}/{total_lutas} lutas baixadas", flush=True)

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

        if not t["was_cached"]:
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


# Lutadores -------------------------------------------------------------------

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

                historico.append(
                    {
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
                    }
                )

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


def baixar_html_lutadores(cfg: PipelineConfig, fighters_index_path: Path, *, dt: str, run_id: str) -> Path:
    client = HttpClient(cfg.http)
    lutadores = read_jsonl(fighters_index_path)

    if cfg.limit_fighters and cfg.limit_fighters > 0:
        lutadores = lutadores[: cfg.limit_fighters]

    out_dir = cfg.data_dir / "raw" / "html" / "lutadores" / f"dt={dt}"
    ensure_dir(out_dir)

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
        1
        for f in lutadores
        if (out_dir / f"{f.get('fighter_id') or fighter_id_from_url(f.get('profile_url'))}.html").exists()
        or (f.get("fighter_id") or fighter_id_from_url(f.get("profile_url") or "")) in ids_em_cache_anterior
    )
    msg = f"[RAW/lutadores] {total} lutadores ({cache_hits} em cache, {total - cache_hits} a baixar) - {workers} workers"
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
            return fid, True
        if fid in ids_em_cache_anterior:
            return fid, True

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

    log.info("[RAW/lutadores] %d baixados, %d ja existiam (pulados).", count, skipped)

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}_fighter_html.json"
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "stage": "raw_fighter_html",
            "rows": count,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )
    return out_dir


def gerar_bronze_lutadores(
    cfg: PipelineConfig,
    fighters_index_path: Path,
    fighter_html_dir: Path,
    *,
    dt: str,
    run_id: str,
) -> Path:
    index = read_jsonl(fighters_index_path)
    if cfg.limit_fighters and cfg.limit_fighters > 0:
        index = index[: cfg.limit_fighters]

    html_root = fighter_html_dir.parent
    html_por_id: dict[str, Path] = {}
    for dt_dir in sorted(html_root.iterdir()):
        if not dt_dir.is_dir():
            continue
        for p in dt_dir.glob("*.html"):
            html_por_id[p.stem] = p

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
    write_json(
        meta_path,
        {
            "run_id": run_id,
            "dt": dt,
            "table": "bronze_lutadores",
            "rows": len(rows),
            "created_at": ingested_at,
        },
    )
    return out_path
