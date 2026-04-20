import argparse
import hashlib
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mssql import NVARCHAR, DATETIME2

from ufc_pipeline.ids import hash5 as _hash5_base
from ufc_pipeline.banco import (
    ConexaoBanco,
    montar_conn_str,
    garantir_tabelas_etl,
    teve_sucesso_anterior,
    registrar_inicio,
    registrar_fim,
    marcar_sucesso,
)
from ufc_pipeline.logger import attach_db_handler

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _hash5(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if value is pd.NA:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return _hash5_base(text_value)


def _sha1(text_value: str) -> str:
    return hashlib.sha1(text_value.encode("utf-8")).hexdigest()


def _read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")
    return pd.read_json(path, lines=True)


LONG_TEXT_COLS = {
    "payload_json",
    "lutadores_json",
    "lutas_json",
    "cartel_texto",
}

SILVER_LUTAS_LUTADORES_COLS = [
    "id_luta",
    "id_evento",
    "nome_evento",
    "ordem_luta",
    "tipo_luta",
    "metodo",
    "id_lutador",
    "fighter_id_hash",
    "nome_lutador",
    "resultado",
    "round_number",
    "sig_str_landed",
    "sig_str_tentados",
    "sig_str_pct",
    "head_landed",
    "head_tentados",
    "body_landed",
    "body_tentados",
    "leg_landed",
    "leg_tentados",
    "distance_landed",
    "distance_tentados",
    "clinch_landed",
    "clinch_tentados",
    "ground_landed",
    "ground_tentados",
    "ingerido_em",
    "dt_carga",
]


def _payload_json(df: pd.DataFrame) -> pd.Series:
    cols = list(df.columns)

    def _row_to_json(row: pd.Series) -> str:
        data = {c: row[c] for c in cols}
        return json.dumps(data, ensure_ascii=False, default=str)

    return df.apply(_row_to_json, axis=1)


def _to_json_safe(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if value is pd.NA:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _ensure_column(df: pd.DataFrame, name: str, default: object = None) -> pd.DataFrame:
    if name not in df.columns:
        df[name] = default
    return df


def _altura_para_cm(valor: object) -> float | None:
    """Converte '5\' 11\"' → cm."""
    if not valor:
        return None
    import re as _re
    m = _re.match(r"(\d+)'\s*(\d+)", str(valor))
    if m:
        return round(int(m.group(1)) * 30.48 + int(m.group(2)) * 2.54, 1)
    return None


def _alcance_para_cm(valor: object) -> float | None:
    """Converte '84\"' → cm."""
    if not valor:
        return None
    import re as _re
    m = _re.search(r"([\d.]+)", str(valor))
    if m:
        return round(float(m.group(1)) * 2.54, 1)
    return None


def _peso_para_lbs(valor: object) -> float | None:
    """Converte '205 lbs.' -> 205."""
    if not valor:
        return None
    m = re.search(r"([\d.]+)", str(valor))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _prepare_bronze_events(df: pd.DataFrame, dt: str) -> pd.DataFrame:
    df = df.copy()
    df["event_id_hash5"] = df["event_id"].apply(_hash5)
    df["dt_carga"] = dt
    return df.rename(columns={
        "name": "nome",
        "event_date": "data_evento",
        "location": "local",
        "ingested_at": "ingerido_em",
    })[
        [
            "event_id",
            "event_id_hash5",
            "nome",
            "data_evento",
            "local",
            "status",
            "ingerido_em",
            "dt_carga",
        ]
    ]


def _prepare_bronze_fights(df: pd.DataFrame, dt: str) -> pd.DataFrame:
    df = df.copy()
    df["fight_id_hash5"] = df["fight_id"].apply(_hash5)
    df["event_id_hash5"] = df["event_id"].apply(_hash5)
    if "bonus_code" in df.columns:
        df["bonus_code"] = pd.to_numeric(df["bonus_code"], errors="coerce").astype("Int64")
    else:
        df["bonus_code"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    if "bout" in df.columns:
        df["bout"] = df["bout"].fillna("normal")
    else:
        df["bout"] = "normal"
    df["lutadores_json"] = df["fighters"].apply(_to_json_safe)
    df["dt_carga"] = dt
    return df.rename(columns={
        "event_name": "nome_evento",
        "bout_order": "ordem_luta",
        "bonus_code": "codigo_bonus",
        "bout": "tipo_luta",
        "method": "metodo",
        "time": "tempo",
        "time_format": "formato_tempo",
        "referee": "arbitro",
        "ingested_at": "ingerido_em",
    })[
        [
            "fight_id",
            "fight_id_hash5",
            "event_id",
            "event_id_hash5",
            "nome_evento",
            "ordem_luta",
            "codigo_bonus",
            "tipo_luta",
            "metodo",
            "round",
            "tempo",
            "formato_tempo",
            "arbitro",
            "lutadores_json",
            "ingerido_em",
            "dt_carga",
        ]
    ]


def _prepare_bronze_fighters(df: pd.DataFrame, dt: str) -> pd.DataFrame:
    df = df.copy()
    if "career_stats" not in df.columns and "stats" in df.columns:
        df["career_stats"] = df["stats"]
    df = _ensure_column(df, "career_stats", None)
    df["fighter_id_hash5"] = df["fighter_id"].apply(_hash5)

    bio_dict = df["bio"].apply(lambda x: x if isinstance(x, dict) else {})
    df["cartel_texto"] = bio_dict.apply(lambda d: d.get("record"))
    parsed = df["cartel_texto"].apply(_parse_record)
    df["vitorias"] = parsed.apply(lambda x: x[0])
    df["derrotas"] = parsed.apply(lambda x: x[1])
    df["empates"] = parsed.apply(lambda x: x[2])
    df["sem_resultado"] = parsed.apply(lambda x: x[3])
    for col in ["vitorias", "derrotas", "empates", "sem_resultado"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")

    df["altura_cm"] = bio_dict.apply(lambda d: _altura_para_cm(d.get("height")))
    df["peso_lbs"] = bio_dict.apply(lambda d: _peso_para_lbs(d.get("weight")))
    df["alcance_cm"] = bio_dict.apply(lambda d: _alcance_para_cm(d.get("reach")))
    df["stance"] = bio_dict.apply(lambda d: d.get("stance"))
    df["data_nascimento"] = bio_dict.apply(lambda d: d.get("dob"))

    stats_dict = df["career_stats"].apply(lambda x: x if isinstance(x, dict) else {})
    df["slpm"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("slpm")))
    df["str_acc_pct"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("str_acc_pct")))
    df["sapm"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("sapm")))
    df["str_def_pct"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("str_def_pct")))
    df["td_avg_15min"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("td_avg_15min")))
    df["td_acc_pct"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("td_acc_pct")))
    df["td_def_pct"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("td_def_pct")))
    df["sub_avg_15min"] = stats_dict.apply(lambda d: _parse_career_metric(d.get("sub_avg_15min")))
    df["lutas_json"] = df["fights"].apply(_to_json_safe)

    df["dt_carga"] = dt
    return df.rename(columns={
        "name": "nome",
        "ingested_at": "ingerido_em",
    })[
        [
            "fighter_id",
            "fighter_id_hash5",
            "nome",
            "cartel_texto",
            "vitorias",
            "derrotas",
            "empates",
            "sem_resultado",
            "altura_cm",
            "peso_lbs",
            "alcance_cm",
            "stance",
            "data_nascimento",
            "slpm",
            "str_acc_pct",
            "sapm",
            "str_def_pct",
            "td_avg_15min",
            "td_acc_pct",
            "td_def_pct",
            "sub_avg_15min",
            "lutas_json",
            "ingerido_em",
            "dt_carga",
        ]
    ]


def _parse_career_stats_from_html(html: str) -> dict[str, str | None]:
    stats: dict[str, str | None] = {
        "slpm": None,
        "str_acc_pct": None,
        "sapm": None,
        "str_def_pct": None,
        "td_avg_15min": None,
        "td_acc_pct": None,
        "td_def_pct": None,
        "sub_avg_15min": None,
    }
    label_to_key = {
        "SLpM:": "slpm",
        "Str. Acc.:": "str_acc_pct",
        "SApM:": "sapm",
        "Str. Def:": "str_def_pct",
        "Str. Def.:": "str_def_pct",
        "TD Avg.:": "td_avg_15min",
        "TD Acc.:": "td_acc_pct",
        "TD Def.:": "td_def_pct",
        "Sub. Avg.:": "sub_avg_15min",
    }

    # Caminho rápido: regex direto no HTML (bem mais rápido que BeautifulSoup por arquivo).
    for label, key in label_to_key.items():
        m = re.search(rf"{re.escape(label)}\s*</i>\s*([^<\r\n]+)", html, flags=re.IGNORECASE)
        if m:
            value = m.group(1).strip()
            stats[key] = value if value else None

    if any(v not in (None, "") for v in stats.values()):
        return stats

    # Fallback resiliente caso a estrutura HTML mude.
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return stats
    soup = BeautifulSoup(html, "html.parser")
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
    for li in soup.select("li.b-list__box-list-item"):
        parts = list(li.stripped_strings)
        if not parts:
            continue
        raw_label = str(parts[0]).strip().rstrip(":").upper()
        key = label_map.get(raw_label)
        if not key:
            continue
        value = str(" ".join(str(p) for p in parts[1:])).strip() if len(parts) > 1 else None
        stats[key] = value
    return stats


def _enriquecer_career_stats_lutadores(df: pd.DataFrame, data_root: Path) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "career_stats" not in df.columns and "stats" in df.columns:
        df["career_stats"] = df["stats"]
    df = _ensure_column(df, "career_stats", None)

    html_root = data_root / "raw" / "html" / "lutadores"
    if not html_root.exists():
        return df

    log.info("[BRONZE] Enriquecendo career stats a partir de HTML local (%d lutadores)...", len(df))

    html_por_fighter: dict[str, Path] = {}
    for dt_dir in sorted(html_root.glob("dt=*")):
        if not dt_dir.is_dir():
            continue
        for p in dt_dir.glob("*.html"):
            html_por_fighter[p.stem] = p
    log.info("[BRONZE] Índice de HTMLs de lutadores carregado: %d arquivos.", len(html_por_fighter))

    enriquecidos = 0
    for i, (idx, row) in enumerate(df.iterrows(), start=1):
        current = row.get("career_stats")
        if isinstance(current, dict) and any(v not in (None, "") for v in current.values()):
            continue
        fighter_id = str(row.get("fighter_id") or "").strip()
        if not fighter_id:
            continue
        html_path = html_por_fighter.get(fighter_id)
        if not html_path or not html_path.exists():
            continue
        try:
            html = html_path.read_text(encoding="utf-8", errors="ignore")
            stats = _parse_career_stats_from_html(html)
            if stats:
                df.at[idx, "career_stats"] = stats
                enriquecidos += 1
        except Exception:
            continue
        if i % 500 == 0 or i == len(df):
            log.info("[BRONZE] Enriquecimento career stats: %d/%d processados...", i, len(df))

    log.info("[BRONZE] Career stats enriquecidos via HTML local para %d lutadores.", enriquecidos)
    return df


def _dedupe_latest(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = df.copy()
    dt_col = "ingerido_em" if "ingerido_em" in df.columns else "ingested_at"
    sort_col = "dt_carga" if "dt_carga" in df.columns else "load_dt" if "load_dt" in df.columns else None
    df["_ingested_at_dt"] = pd.to_datetime(df.get(dt_col), errors="coerce")
    sort_by = [key_col, "_ingested_at_dt"] + ([sort_col] if sort_col and sort_col in df.columns else [])
    asc = [True, False] + ([False] if sort_col and sort_col in df.columns else [])
    df = df.sort_values(sort_by, ascending=asc)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    return df.drop(columns=["_ingested_at_dt"])


def _extract_record_text(bio_json: object) -> str | None:
    if bio_json is None:
        return None
    if isinstance(bio_json, float) and pd.isna(bio_json):
        return None
    if isinstance(bio_json, dict):
        return bio_json.get("record")
    if isinstance(bio_json, str):
        try:
            data = json.loads(bio_json)
        except json.JSONDecodeError:
            return None
        if isinstance(data, dict):
            return data.get("record")
    return None


def _parse_record(record_text: str | None) -> tuple[int | None, int | None, int | None, int | None]:
    if not record_text:
        return None, None, None, None
    base = re.sub(r"\s*\(.*$", "", record_text)
    wins = re.search(r"^(\d+)-", base)
    losses = re.search(r"^\d+-(\d+)", base)
    draws = re.search(r"^\d+-\d+-(\d+)", base)
    nc = re.search(r"\((\d+)\s*NC\)", record_text.upper())
    return (
        int(wins.group(1)) if wins else None,
        int(losses.group(1)) if losses else None,
        int(draws.group(1)) if draws else None,
        int(nc.group(1)) if nc else None,
    )


def _bio_to_dict(bio_json: object) -> dict:
    data = _safe_json_load(bio_json)
    return data if isinstance(data, dict) else {}


def _parse_career_metric(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    text_value = text_value.replace("%", "").replace(",", ".")
    m = re.search(r"(-?\d+(?:\.\d+)?)", text_value)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _build_silver_from_bronze(
    bronze_events: pd.DataFrame,
    bronze_fights: pd.DataFrame,
    bronze_fighters: pd.DataFrame,
    id_maps: tuple[dict[str, int], dict[str, int], dict[str, int]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    evento_map, luta_map, lutador_map = id_maps

    base_ev = _dedupe_latest(bronze_events, "event_id").copy()
    base_ev["id_evento"] = base_ev["event_id"].map(evento_map).astype("Int64")
    base_ev = _ensure_column(base_ev, "nome", None)
    base_ev = _ensure_column(base_ev, "data_evento", None)
    base_ev = _ensure_column(base_ev, "local", None)
    base_ev = _ensure_column(base_ev, "status", None)
    base_ev = _ensure_column(base_ev, "ingerido_em", None)
    base_ev = _ensure_column(base_ev, "dt_carga", None)
    silver_events = base_ev[[
        "id_evento", "nome", "data_evento",
        "local", "status", "ingerido_em", "dt_carga",
    ]]

    base_fi = _dedupe_latest(bronze_fights, "fight_id").copy()
    base_fi["id_luta"] = base_fi["fight_id"].map(luta_map).astype("Int64")
    base_fi["id_evento"] = base_fi["event_id"].map(evento_map).astype("Int64")
    if "codigo_bonus" in base_fi.columns:
        base_fi["codigo_bonus"] = pd.to_numeric(base_fi["codigo_bonus"], errors="coerce").astype("Int64")
    else:
        base_fi["codigo_bonus"] = pd.Series([pd.NA] * len(base_fi), dtype="Int64")
    base_fi["tipo_luta"] = base_fi.get("tipo_luta", pd.Series(["normal"] * len(base_fi))).fillna("normal")
    base_fi = _ensure_column(base_fi, "metodo", None)
    base_fi = _ensure_column(base_fi, "round", None)
    base_fi = _ensure_column(base_fi, "tempo", None)
    base_fi = _ensure_column(base_fi, "formato_tempo", None)
    base_fi = _ensure_column(base_fi, "arbitro", None)
    base_fi = _ensure_column(base_fi, "lutadores_json", None)
    base_fi = _ensure_column(base_fi, "ingerido_em", None)
    base_fi = _ensure_column(base_fi, "dt_carga", None)
    silver_fights = base_fi[[
        "id_luta", "id_evento", "nome_evento", "ordem_luta",
        "codigo_bonus", "tipo_luta", "metodo", "round", "tempo",
        "formato_tempo", "arbitro", "lutadores_json", "ingerido_em", "dt_carga",
    ]]

    base_lt = _dedupe_latest(bronze_fighters, "fighter_id").copy()
    base_lt["id_lutador"] = base_lt["fighter_id"].map(lutador_map).astype("Int64")
    base_lt = _ensure_column(base_lt, "nome", None)
    base_lt = _ensure_column(base_lt, "cartel_texto", None)
    base_lt = _ensure_column(base_lt, "vitorias", pd.NA)
    base_lt = _ensure_column(base_lt, "derrotas", pd.NA)
    base_lt = _ensure_column(base_lt, "empates", pd.NA)
    base_lt = _ensure_column(base_lt, "sem_resultado", pd.NA)
    base_lt = _ensure_column(base_lt, "altura_cm", None)
    base_lt = _ensure_column(base_lt, "peso_lbs", None)
    base_lt = _ensure_column(base_lt, "alcance_cm", None)
    base_lt = _ensure_column(base_lt, "stance", None)
    base_lt = _ensure_column(base_lt, "data_nascimento", None)
    base_lt = _ensure_column(base_lt, "slpm", None)
    base_lt = _ensure_column(base_lt, "str_acc_pct", None)
    base_lt = _ensure_column(base_lt, "sapm", None)
    base_lt = _ensure_column(base_lt, "str_def_pct", None)
    base_lt = _ensure_column(base_lt, "td_avg_15min", None)
    base_lt = _ensure_column(base_lt, "td_acc_pct", None)
    base_lt = _ensure_column(base_lt, "td_def_pct", None)
    base_lt = _ensure_column(base_lt, "sub_avg_15min", None)
    base_lt = _ensure_column(base_lt, "ingerido_em", None)
    base_lt = _ensure_column(base_lt, "dt_carga", None)

    # Fallback para bronze antigo que ainda tinha bio_json/stats_json.
    if "bio_json" in base_lt.columns:
        missing_record = base_lt["cartel_texto"].isna() | (base_lt["cartel_texto"].astype(str).str.strip() == "")
        base_lt.loc[missing_record, "cartel_texto"] = base_lt.loc[missing_record, "bio_json"].apply(_extract_record_text)

        parsed = base_lt["cartel_texto"].apply(_parse_record)
        for col, idx in [("vitorias", 0), ("derrotas", 1), ("empates", 2), ("sem_resultado", 3)]:
            miss = base_lt[col].isna()
            base_lt.loc[miss, col] = parsed.loc[miss].apply(lambda x: x[idx])

        bio_dict = base_lt["bio_json"].apply(_bio_to_dict)
        miss_altura = base_lt["altura_cm"].isna()
        miss_alcance = base_lt["alcance_cm"].isna()
        miss_peso = base_lt["peso_lbs"].isna()
        miss_stance = base_lt["stance"].isna() | (base_lt["stance"].astype(str).str.strip() == "")
        miss_dob = base_lt["data_nascimento"].isna() | (base_lt["data_nascimento"].astype(str).str.strip() == "")
        base_lt.loc[miss_altura, "altura_cm"] = bio_dict.loc[miss_altura].apply(lambda d: _altura_para_cm(d.get("height")))
        base_lt.loc[miss_alcance, "alcance_cm"] = bio_dict.loc[miss_alcance].apply(lambda d: _alcance_para_cm(d.get("reach")))
        base_lt.loc[miss_peso, "peso_lbs"] = bio_dict.loc[miss_peso].apply(lambda d: _peso_para_lbs(d.get("weight")))
        base_lt.loc[miss_stance, "stance"] = bio_dict.loc[miss_stance].apply(lambda d: d.get("stance"))
        base_lt.loc[miss_dob, "data_nascimento"] = bio_dict.loc[miss_dob].apply(lambda d: d.get("dob"))

    if "stats_json" in base_lt.columns:
        stats_dict = base_lt["stats_json"].apply(_safe_json_load)
        for col in ["slpm", "str_acc_pct", "sapm", "str_def_pct", "td_avg_15min", "td_acc_pct", "td_def_pct", "sub_avg_15min"]:
            miss = base_lt[col].isna()
            base_lt.loc[miss, col] = stats_dict.loc[miss].apply(
                lambda d: _parse_career_metric(d.get(col)) if isinstance(d, dict) else None
            )

    for col in ["vitorias", "derrotas", "empates", "sem_resultado"]:
        base_lt[col] = pd.to_numeric(base_lt[col], errors="coerce").fillna(0).astype("Int64")
    for col in [
        "altura_cm", "peso_lbs", "alcance_cm",
        "slpm", "str_acc_pct", "sapm", "str_def_pct",
        "td_avg_15min", "td_acc_pct", "td_def_pct", "sub_avg_15min",
    ]:
        base_lt[col] = pd.to_numeric(base_lt[col], errors="coerce")

    silver_fighters = base_lt[[
        "id_lutador", "nome", "cartel_texto",
        "vitorias", "derrotas", "empates", "sem_resultado",
        "altura_cm", "peso_lbs", "alcance_cm", "stance", "data_nascimento",
        "slpm", "str_acc_pct", "sapm", "str_def_pct",
        "td_avg_15min", "td_acc_pct", "td_def_pct", "sub_avg_15min",
        "ingerido_em", "dt_carga",
    ]]
    return silver_events, silver_fights, silver_fighters


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def _fight_time_seconds(value: object) -> int | None:
    if not value:
        return None
    try:
        minutes, seconds = str(value).split(":")
        return int(minutes) * 60 + int(seconds)
    except (ValueError, TypeError):
        return None


def _safe_json_load(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _parse_landed_tentados(value: object) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    if isinstance(value, float) and pd.isna(value):
        return None, None
    text_value = str(value).strip()
    if not text_value:
        return None, None
    match = re.search(r"(\d+)\s*of\s*(\d+)", text_value, flags=re.IGNORECASE)
    if match:
        try:
            return int(match.group(1)), int(match.group(2))
        except ValueError:
            return None, None
    one_number = re.search(r"(\d+)", text_value)
    if one_number:
        try:
            return int(one_number.group(1)), None
        except ValueError:
            return None, None
    return None, None


def _build_silver_lutas_lutadores(
    silver_fights: pd.DataFrame,
    id_maps: tuple[dict[str, int], dict[str, int], dict[str, int]],
) -> pd.DataFrame:
    _, _, lutador_map = id_maps
    rows: list[dict] = []

    for fight in silver_fights.to_dict(orient="records"):
        fighters = _safe_json_load(fight.get("lutadores_json"))
        if not isinstance(fighters, list):
            continue

        for slot, fighter in enumerate(fighters, start=1):
            if not isinstance(fighter, dict):
                continue

            fighter_hash = str(fighter.get("fighter_id") or "").strip()
            if not fighter_hash:
                fighter_hash = _sha1(f"{fight.get('id_luta')}:{slot}:{fighter.get('name') or ''}")
            id_lutador = lutador_map.get(fighter_hash)
            rounds_data = fighter.get("rounds_sig_strikes")

            rounds_registrados = False
            if isinstance(rounds_data, dict) and rounds_data:
                for round_key, round_stats in rounds_data.items():
                    if not isinstance(round_stats, dict):
                        continue
                    rounds_registrados = True

                    sig_landed, sig_tentados = _parse_landed_tentados(round_stats.get("sig_str"))
                    head_landed, head_tentados = _parse_landed_tentados(round_stats.get("head"))
                    body_landed, body_tentados = _parse_landed_tentados(round_stats.get("body"))
                    leg_landed, leg_tentados = _parse_landed_tentados(round_stats.get("leg"))
                    distance_landed, distance_tentados = _parse_landed_tentados(round_stats.get("distance"))
                    clinch_landed, clinch_tentados = _parse_landed_tentados(round_stats.get("clinch"))
                    ground_landed, ground_tentados = _parse_landed_tentados(round_stats.get("ground"))

                    rows.append(
                        {
                            "id_luta": fight.get("id_luta"),
                            "id_evento": fight.get("id_evento"),
                            "nome_evento": fight.get("nome_evento"),
                            "ordem_luta": fight.get("ordem_luta"),
                            "tipo_luta": fight.get("tipo_luta"),
                            "metodo": fight.get("metodo"),
                            "id_lutador": id_lutador,
                            "fighter_id_hash": fighter_hash,
                            "nome_lutador": fighter.get("name"),
                            "resultado": str(fighter.get("result") or "").upper() or None,
                            "round_number": _safe_int(round_key),
                            "sig_str_landed": sig_landed,
                            "sig_str_tentados": sig_tentados,
                            "sig_str_pct": _parse_career_metric(round_stats.get("sig_str_pct")),
                            "head_landed": head_landed,
                            "head_tentados": head_tentados,
                            "body_landed": body_landed,
                            "body_tentados": body_tentados,
                            "leg_landed": leg_landed,
                            "leg_tentados": leg_tentados,
                            "distance_landed": distance_landed,
                            "distance_tentados": distance_tentados,
                            "clinch_landed": clinch_landed,
                            "clinch_tentados": clinch_tentados,
                            "ground_landed": ground_landed,
                            "ground_tentados": ground_tentados,
                            "ingerido_em": fight.get("ingerido_em"),
                            "dt_carga": fight.get("dt_carga"),
                        }
                    )

            if not rounds_registrados:
                rows.append(
                    {
                        "id_luta": fight.get("id_luta"),
                        "id_evento": fight.get("id_evento"),
                        "nome_evento": fight.get("nome_evento"),
                        "ordem_luta": fight.get("ordem_luta"),
                        "tipo_luta": fight.get("tipo_luta"),
                        "metodo": fight.get("metodo"),
                        "id_lutador": id_lutador,
                        "fighter_id_hash": fighter_hash,
                        "nome_lutador": fighter.get("name"),
                        "resultado": str(fighter.get("result") or "").upper() or None,
                        "round_number": 0,
                        "sig_str_landed": None,
                        "sig_str_tentados": None,
                        "sig_str_pct": None,
                        "head_landed": None,
                        "head_tentados": None,
                        "body_landed": None,
                        "body_tentados": None,
                        "leg_landed": None,
                        "leg_tentados": None,
                        "distance_landed": None,
                        "distance_tentados": None,
                        "clinch_landed": None,
                        "clinch_tentados": None,
                        "ground_landed": None,
                        "ground_tentados": None,
                        "ingerido_em": fight.get("ingerido_em"),
                        "dt_carga": fight.get("dt_carga"),
                    }
                )

    silver_lutas_lutadores = pd.DataFrame(rows, columns=SILVER_LUTAS_LUTADORES_COLS)
    if silver_lutas_lutadores.empty:
        return silver_lutas_lutadores

    for col in ["id_luta", "id_evento", "id_lutador", "ordem_luta", "round_number"]:
        silver_lutas_lutadores[col] = pd.to_numeric(silver_lutas_lutadores[col], errors="coerce").astype("Int64")
    for col in [
        "sig_str_landed", "sig_str_tentados", "head_landed", "head_tentados",
        "body_landed", "body_tentados", "leg_landed", "leg_tentados",
        "distance_landed", "distance_tentados", "clinch_landed", "clinch_tentados",
        "ground_landed", "ground_tentados",
    ]:
        silver_lutas_lutadores[col] = pd.to_numeric(silver_lutas_lutadores[col], errors="coerce").astype("Int64")
    silver_lutas_lutadores["sig_str_pct"] = pd.to_numeric(silver_lutas_lutadores["sig_str_pct"], errors="coerce")

    return silver_lutas_lutadores


def _build_gold_metrics(
    silver_events: pd.DataFrame,
    silver_fights: pd.DataFrame,
    silver_fighters: pd.DataFrame,
    id_maps: tuple[dict[str, int], dict[str, int], dict[str, int]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _, luta_map, lutador_map = id_maps

    rows: list[dict] = []
    for fight in silver_fights.to_dict(orient="records"):
        fighters = _safe_json_load(fight.get("lutadores_json"))
        if not isinstance(fighters, list):
            continue
        round_int = _safe_int(fight.get("round"))
        fight_time = _fight_time_seconds(fight.get("tempo"))
        id_luta = fight.get("id_luta")
        id_evento = fight.get("id_evento")
        for fighter in fighters:
            if not isinstance(fighter, dict):
                continue
            fhash = str(fighter.get("fighter_id") or "")
            id_lutador = lutador_map.get(fhash)
            rows.append(
                {
                    "id_evento":          id_evento,
                    "id_luta":            id_luta,
                    "metodo":             fight.get("metodo"),
                    "round_int":          round_int,
                    "fight_time_seconds": fight_time,
                    "id_lutador":         id_lutador,
                    "nome_lutador":       fighter.get("name"),
                    "resultado":          str(fighter.get("result") or "").upper(),
                    "rounds_sig_strikes": fighter.get("rounds_sig_strikes") or {},
                }
            )
    ff = pd.DataFrame(rows)
    if ff.empty:
        ff = pd.DataFrame(columns=[
            "id_evento", "id_luta", "metodo", "round_int", "fight_time_seconds",
            "id_lutador", "nome_lutador", "resultado", "rounds_sig_strikes",
        ])

    round_rows: list[dict] = []
    for row in ff.to_dict(orient="records"):
        rounds = row.get("rounds_sig_strikes")
        if not isinstance(rounds, dict):
            continue
        for key, value in rounds.items():
            if not isinstance(value, dict):
                continue
            round_rows.append(
                {
                    "id_evento":   row.get("id_evento"),
                    "id_luta":     row.get("id_luta"),
                    "id_lutador":  row.get("id_lutador"),
                    "round_number": _safe_int(key),
                    "sig_str":     value.get("sig_str"),
                    "sig_str_pct": value.get("sig_str_pct"),
                }
            )
    round_stats = pd.DataFrame(round_rows)
    if round_stats.empty:
        round_stats = pd.DataFrame(columns=[
            "id_evento", "id_luta", "id_lutador", "round_number",
            "sig_str", "sig_str_pct", "sig_str_landed", "sig_str_pct_num",
        ])
    else:
        sig_landed = round_stats["sig_str"].fillna("").str.split(" of ").str.get(0)
        sig_pct = round_stats["sig_str_pct"].fillna("").str.replace("%", "", regex=False)
        round_stats["sig_str_landed"] = pd.to_numeric(sig_landed, errors="coerce")
        round_stats["sig_str_pct_num"] = pd.to_numeric(sig_pct, errors="coerce")

    if round_stats.empty:
        fighter_perf = pd.DataFrame(columns=[
            "id_evento", "id_luta", "id_lutador",
            "sig_str_landed_total", "sig_str_pct_avg", "rounds_fought",
        ])
    else:
        fighter_perf = (
            round_stats.groupby(["id_evento", "id_luta", "id_lutador"], dropna=False)
            .agg(
                sig_str_landed_total=("sig_str_landed", "sum"),
                sig_str_pct_avg=("sig_str_pct_num", "mean"),
                rounds_fought=("round_number", "nunique"),
            )
            .reset_index()
        )

    ff_enriched = ff.merge(fighter_perf, on=["id_evento", "id_luta", "id_lutador"], how="left")

    method = ff_enriched["metodo"].fillna("")
    has_method = method != ""
    is_decision = method.str.startswith("Decision")
    is_submission = method.str.startswith("Submission")
    is_ko_tko = method.str.contains("KO/TKO", na=False)
    is_finish = has_method & ~is_decision
    ff_enriched["is_decision"] = is_decision
    ff_enriched["is_submission"] = is_submission
    ff_enriched["is_ko_tko"] = is_ko_tko
    ff_enriched["is_finish"] = is_finish

    event_metrics = (
        ff_enriched.groupby("id_evento", dropna=False)
        .agg(
            m01_total_fights=("id_luta", "nunique"),
            m02_total_fighters=("id_lutador", "nunique"),
            m07_avg_round=("round_int", "mean"),
            m08_avg_fight_time_seconds=("fight_time_seconds", "mean"),
            m09_avg_sig_str_landed_per_fighter=("sig_str_landed_total", "mean"),
            m10_avg_sig_str_pct_per_fighter=("sig_str_pct_avg", "mean"),
            m03_total_finishes=("is_finish", "sum"),
            m04_total_decisions=("is_decision", "sum"),
            m05_total_submissions=("is_submission", "sum"),
            m06_total_ko_tko=("is_ko_tko", "sum"),
        )
        .reset_index()
    )
    event_metrics["m11_finish_rate_pct"] = (
        (event_metrics["m03_total_finishes"] / event_metrics["m01_total_fights"]) * 100.0
    ).round(2)
    event_metrics = event_metrics.merge(
        silver_events[["id_evento", "nome", "data_evento", "local"]].rename(columns={"nome": "nome_evento"}),
        on="id_evento",
        how="left",
    )

    result = ff_enriched["resultado"].fillna("")
    is_win = result == "W"
    is_loss = result == "L"
    is_draw = result == "D"
    is_nc = result == "NC"
    ff_enriched["is_win"] = is_win
    ff_enriched["is_loss"] = is_loss
    ff_enriched["is_draw"] = is_draw
    ff_enriched["is_nc"] = is_nc
    ff_enriched["is_finish_win"] = is_win & is_finish
    ff_enriched["is_decision_win"] = is_win & is_decision

    fighter_metrics = (
        ff_enriched.groupby("id_lutador", dropna=False)
        .agg(
            m01_fights_total=("id_luta", "nunique"),
            m02_wins=("is_win", "sum"),
            m03_losses=("is_loss", "sum"),
            m04_draws=("is_draw", "sum"),
            m05_no_contests=("is_nc", "sum"),
            m06_finish_wins=("is_finish_win", "sum"),
            m07_decision_wins=("is_decision_win", "sum"),
            m08_avg_round_reached=("round_int", "mean"),
            m09_avg_fight_time_seconds=("fight_time_seconds", "mean"),
            m10_avg_sig_str_landed=("sig_str_landed_total", "mean"),
            m11_avg_sig_str_pct=("sig_str_pct_avg", "mean"),
        )
        .reset_index()
    )
    fighter_metrics["m12_win_rate_pct"] = (
        (fighter_metrics["m02_wins"] / fighter_metrics["m01_fights_total"]) * 100.0
    ).round(2)
    fighter_metrics = fighter_metrics.merge(
        silver_fighters[["id_lutador", "nome", "cartel_texto", "vitorias", "derrotas", "empates", "sem_resultado"]]
        .rename(columns={"nome": "nome_lutador"}),
        on="id_lutador",
        how="left",
    )
    return event_metrics, fighter_metrics


# ── Mapa de IDs sequenciais ───────────────────────────────────────────────────

def _popular_mapa_ids(
    engine,
    bronze_events: pd.DataFrame,
    bronze_fights: pd.DataFrame,
    bronze_fighters: pd.DataFrame,
) -> None:
    """Insere/mantém etl.mapa_ids com IDs sequenciais para cada entidade.
    Usa bronze (staging) para garantir que todos os hashes existam antes de
    construir a camada silver com IDs inteiros.
    """
    rows: list[tuple[str, str, str]] = []

    for _, r in bronze_events.iterrows():
        hid = str(r.get("event_id") or "")
        nome = str(r.get("nome") or "")
        if hid:
            rows.append(("evento", hid, nome))

    for _, r in bronze_fights.iterrows():
        hid = str(r.get("fight_id") or "")
        nome = str(r.get("nome_evento") or "")
        if hid:
            rows.append(("luta", hid, nome))

    # Lutadores do bronze_fighters (tem perfil completo)
    seen_fids: set[str] = set()
    for _, r in bronze_fighters.iterrows():
        hid = str(r.get("fighter_id") or "")
        nome = str(r.get("nome") or "")
        if hid:
            rows.append(("lutador", hid, nome))
            seen_fids.add(hid)

    # Lutadores extras extraídos dos JSONs de lutas (sem perfil próprio)
    for _, r in bronze_fights.iterrows():
        fighters = _safe_json_load(r.get("lutadores_json"))
        if not isinstance(fighters, list):
            continue
        for f in fighters:
            if not isinstance(f, dict):
                continue
            fid = str(f.get("fighter_id") or "")
            if fid and fid not in seen_fids:
                rows.append(("lutador", fid, str(f.get("name") or "")))
                seen_fids.add(fid)

    if not rows:
        return

    stg = f"etl.__stg_mapa_{uuid.uuid4().hex[:8]}"
    create_stg = (
        f"CREATE TABLE {stg} "
        "(tipo NVARCHAR(20) NOT NULL, hash_id NVARCHAR(200) NOT NULL, nome NVARCHAR(400) NULL)"
    )
    with engine.begin() as conn:
        conn.execute(text(create_stg))

    conn_raw = engine.raw_connection()
    try:
        cursor = conn_raw.cursor()
        cursor.fast_executemany = True
        cursor.executemany(
            f"INSERT INTO {stg} (tipo, hash_id, nome) VALUES (?, ?, ?)",
            rows,
        )
        conn_raw.commit()
    finally:
        conn_raw.close()

    merge_sql = (
        "SET NOCOUNT ON; "
        "MERGE etl.mapa_ids AS tgt "
        f"USING {stg} AS src ON tgt.tipo = src.tipo AND tgt.hash_id = src.hash_id "
        "WHEN MATCHED AND src.nome IS NOT NULL AND src.nome <> '' "
        "  THEN UPDATE SET tgt.nome = src.nome "
        "WHEN NOT MATCHED BY TARGET "
        "  THEN INSERT (tipo, hash_id, nome) VALUES (src.tipo, src.hash_id, src.nome);"
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"IF OBJECT_ID('{stg}','U') IS NOT NULL DROP TABLE {stg};"))

    log.info("[MAPA_IDS] %d entidades registradas em etl.mapa_ids.", len(rows))


def _ler_mapa_ids(engine) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """Lê etl.mapa_ids e retorna três dicts hash→id: (eventos, lutas, lutadores)."""
    df = pd.read_sql("SELECT tipo, hash_id, id FROM etl.mapa_ids", engine)
    def _to_map(tipo: str) -> dict[str, int]:
        sub = df[df["tipo"] == tipo]
        return dict(zip(sub["hash_id"], sub["id"].astype(int)))
    return _to_map("evento"), _to_map("luta"), _to_map("lutador")


# ── SQL Server helpers ────────────────────────────────────────────────────────

def _quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _split_table_name(full_name: str) -> tuple[str, str]:
    parts = [p.strip().strip("[]") for p in full_name.split(".") if p.strip()]
    if len(parts) == 1:
        return "dbo", parts[0]
    return parts[-2], parts[-1]


def _create_engine(banco: ConexaoBanco):
    conn_str = montar_conn_str(banco)
    if not conn_str:
        driver = banco.odbc_driver or "ODBC Driver 17 for SQL Server"
        conn_str = montar_conn_str(banco, driver=driver)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        fast_executemany=True,
        use_setinputsizes=False,
    )


def _ensure_schema(engine, schema: str) -> None:
    query = text("IF SCHEMA_ID(:schema) IS NULL EXEC('CREATE SCHEMA ' + :schema)")
    with engine.begin() as conn:
        conn.execute(query, {"schema": schema})


def _table_exists(engine, schema: str, table: str) -> bool:
    query = text(
        "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :schema AND t.name = :table"
    )
    with engine.begin() as conn:
        res = conn.execute(query, {"schema": schema, "table": table}).first()
    return res is not None


def _drop_table_if_exists(engine, schema: str, table: str) -> None:
    sql = text(
        f"IF OBJECT_ID('{schema}.{table}','U') IS NOT NULL "
        f"DROP TABLE {_quote_ident(schema)}.{_quote_ident(table)};"
    )
    try:
        with engine.begin() as conn:
            conn.execute(sql)
    except Exception as exc:
        log.warning("[SCHEMA] Não foi possível remover %s.%s: %s", schema, table, exc)


def _drop_columns_if_exist(engine, schema: str, table: str, columns: Iterable[str]) -> None:
    cols = [c for c in columns if c]
    if not cols:
        return
    if not _table_exists(engine, schema, table):
        return
    query = text(
        "SELECT c.name "
        "FROM sys.columns c "
        "JOIN sys.tables t ON c.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :schema AND t.name = :table"
    )
    with engine.begin() as conn:
        existing_rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
        existing = {str(r[0]).lower() for r in existing_rows}
        to_drop = [c for c in cols if c.lower() in existing]
        for col in to_drop:
            try:
                conn.execute(text(
                    f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} DROP COLUMN {_quote_ident(col)};"
                ))
            except Exception as exc:
                log.warning("[SCHEMA] Não foi possível remover coluna %s.%s.%s: %s", schema, table, col, exc)


def _read_table(engine, full_name: str) -> pd.DataFrame:
    schema, table = _split_table_name(full_name)
    return pd.read_sql(f"SELECT * FROM {_quote_ident(schema)}.{_quote_ident(table)}", engine)


def _sql_type_for_column(col: str, series: pd.Series) -> str:
    if col in LONG_TEXT_COLS:
        return "NVARCHAR(MAX)"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME2"
    if pd.api.types.is_bool_dtype(series):
        return "BIT"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    return "NVARCHAR(4000)"


def _ensure_table_columns(engine, schema: str, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    query = text(
        "SELECT c.name "
        "FROM sys.columns c "
        "JOIN sys.tables t ON c.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :schema AND t.name = :table"
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
    existing = {str(r[0]).lower() for r in rows}
    missing = [c for c in df.columns if c.lower() not in existing]
    if not missing:
        return
    with engine.begin() as conn:
        for col in missing:
            sql_type = _sql_type_for_column(col, df[col])
            alter_sql = (
                f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} "
                f"ADD {_quote_ident(col)} {sql_type} NULL;"
            )
            conn.execute(text(alter_sql))


def _sql_dtypes_for(df: pd.DataFrame) -> dict:
    dtype: dict = {}
    for col in df.columns:
        if col in LONG_TEXT_COLS:
            dtype[col] = NVARCHAR(None)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            dtype[col] = DATETIME2()
    return dtype


def _normalize_datetime_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def _ensure_text_columns(engine, schema: str, table: str, columns: Iterable[str]) -> None:
    cols = [c for c in columns if c in LONG_TEXT_COLS]
    if not cols:
        return
    col_list = ", ".join([f"'{c}'" for c in cols])
    obj_name = f"{schema}.{table}"
    query = (
        "SELECT c.name, t.name AS type_name, c.max_length "
        "FROM sys.columns c "
        "JOIN sys.types t ON c.user_type_id = t.user_type_id "
        f"WHERE c.object_id = OBJECT_ID(:obj) AND c.name IN ({col_list});"
    )
    with engine.begin() as conn:
        rows = conn.execute(text(query), {"obj": obj_name}).fetchall()
        if not rows:
            return
        for name, type_name, max_length in rows:
            if type_name != "nvarchar" or max_length != -1:
                alter_sql = (
                    f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} "
                    f"ALTER COLUMN {_quote_ident(name)} NVARCHAR(MAX);"
                )
                conn.execute(text(alter_sql))


def _rowversion_columns(engine, schema: str, table: str, columns: Iterable[str]) -> list[str]:
    cols = list(columns)
    if not cols:
        return []
    col_list = ", ".join([f"'{c}'" for c in cols])
    obj_name = f"{schema}.{table}"
    query = (
        "SELECT c.name "
        "FROM sys.columns c "
        "JOIN sys.types t ON c.user_type_id = t.user_type_id "
        "WHERE c.object_id = OBJECT_ID(:obj) "
        "AND c.name IN (" + col_list + ") "
        "AND t.name IN ('timestamp','rowversion');"
    )
    with engine.begin() as conn:
        rows = conn.execute(text(query), {"obj": obj_name}).fetchall()
    return [r[0] for r in rows]


def _table_rowcount(engine, schema: str, table: str) -> int:
    query = text(f"SELECT COUNT(1) FROM {_quote_ident(schema)}.{_quote_ident(table)}")
    with engine.begin() as conn:
        return int(conn.execute(query).scalar() or 0)


def _create_table_from_df(engine, schema: str, table: str, df: pd.DataFrame) -> None:
    df.head(0).to_sql(table, engine, schema=schema, if_exists="replace", index=False, dtype=_sql_dtypes_for(df))


def _insert_rows(engine, schema: str, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    import pyodbc

    def _to_native(value: object) -> object:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        try:
            import numpy as np
            if isinstance(value, np.generic):
                return value.item()
        except Exception:
            pass
        return value

    safe_df = df.astype(object).where(pd.notna(df), None)
    cols = list(safe_df.columns)
    col_list = ", ".join([_quote_ident(c) for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO {_quote_ident(schema)}.{_quote_ident(table)} ({col_list}) VALUES ({placeholders})"

    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        has_long_text = any(col in LONG_TEXT_COLS for col in cols)
        rows = [tuple(_to_native(v) for v in row) for row in safe_df.itertuples(index=False, name=None)]
        if has_long_text:
            cursor.fast_executemany = False
            for row in rows:
                cursor.execute(sql, row)
        else:
            cursor.fast_executemany = True
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    return len(safe_df)


def _merge_sqlserver_by_key(
    df: pd.DataFrame,
    *,
    engine,
    table_name: str,
    keys: Iterable[str],
) -> int:
    df = _normalize_datetime_cols(df)
    key_cols = list(keys)
    if not key_cols:
        raise ValueError("keys nao pode ser vazio")
    if df.empty:
        return 0
    schema, table = _split_table_name(table_name)
    _ensure_schema(engine, schema)
    if not _table_exists(engine, schema, table):
        _create_table_from_df(engine, schema, table, df)
        return _insert_rows(engine, schema, table, df)

    rv_cols = _rowversion_columns(engine, schema, table, df.columns)
    if rv_cols:
        rowcount = _table_rowcount(engine, schema, table)
        if rowcount > 0:
            raise RuntimeError(
                f"Tabela {schema}.{table} possui coluna rowversion ({', '.join(rv_cols)}). "
                "Remova/recrie a tabela para corrigir o schema."
            )
        drop_sql = f"IF OBJECT_ID('{schema}.{table}','U') IS NOT NULL DROP TABLE {_quote_ident(schema)}.{_quote_ident(table)};"
        with engine.begin() as conn:
            conn.execute(text(drop_sql))
        _create_table_from_df(engine, schema, table, df)
        return _insert_rows(engine, schema, table, df)

    _ensure_table_columns(engine, schema, table, df)
    _ensure_text_columns(engine, schema, table, df.columns)

    stg_table = f"__stg_{table}_{uuid.uuid4().hex[:10]}"
    _create_table_from_df(engine, schema, stg_table, df)
    _insert_rows(engine, schema, stg_table, df)
    cols = list(df.columns)
    non_key_cols = [c for c in cols if c not in key_cols]
    tgt = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    stg = f"{_quote_ident(schema)}.{_quote_ident(stg_table)}"
    on_clause = " AND ".join([f"tgt.{_quote_ident(k)} = src.{_quote_ident(k)}" for k in key_cols])
    insert_cols = ", ".join([_quote_ident(c) for c in cols])
    insert_vals = ", ".join([f"src.{_quote_ident(c)}" for c in cols])
    if non_key_cols:
        update_clause = ", ".join([f"tgt.{_quote_ident(c)} = src.{_quote_ident(c)}" for c in non_key_cols])
        merge_sql = (
            "SET NOCOUNT ON; "
            f"MERGE {tgt} AS tgt "
            f"USING {stg} AS src "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_clause} "
            f"WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )
    else:
        merge_sql = (
            "SET NOCOUNT ON; "
            f"MERGE {tgt} AS tgt "
            f"USING {stg} AS src "
            f"ON {on_clause} "
            f"WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )
    try:
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
    finally:
        drop_sql = f"IF OBJECT_ID('{schema}.{stg_table}','U') IS NOT NULL DROP TABLE {stg};"
        with engine.begin() as conn:
            conn.execute(text(drop_sql))
    return len(df)


def _upsert_by_key(
    df: pd.DataFrame,
    *,
    engine,
    banco: ConexaoBanco,
    table_key: str,
    keys: Iterable[str],
) -> int:
    table_name = banco.tabela(table_key)
    return _merge_sqlserver_by_key(df, engine=engine, table_name=table_name, keys=keys)


def _resolve_banco(args: argparse.Namespace) -> ConexaoBanco:
    base = ConexaoBanco.do_env()
    return ConexaoBanco(
        server=args.server or base.server,
        database=args.database or base.database,
        schema=args.schema or base.schema,
        bronze_schema=args.bronze_schema or base.bronze_schema,
        silver_schema=args.silver_schema or base.silver_schema,
        gold_schema=args.gold_schema or base.gold_schema,
        user=args.user or base.user,
        password=args.password or base.password,
        encrypt=args.encrypt if args.encrypt is not None else base.encrypt,
        trust_server_certificate=(
            args.trust_server_certificate
            if args.trust_server_certificate is not None
            else base.trust_server_certificate
        ),
        odbc_driver=args.odbc_driver or base.odbc_driver,
        odbc_extra=args.odbc_extra or base.odbc_extra,
        table_map=base.table_map,
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)

    parser = argparse.ArgumentParser(
        description="Loader SQL Server: Bronze -> Silver -> Gold"
    )
    parser.add_argument("--dt", required=True, help="Particao dt no formato YYYY-MM-DD")
    parser.add_argument("--data-root", default="./data")
    parser.add_argument("--server", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--bronze-schema", default=None)
    parser.add_argument("--silver-schema", default=None)
    parser.add_argument("--gold-schema", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--encrypt", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--trust-server-certificate", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--odbc-driver", default=None)
    parser.add_argument("--odbc-extra", default=None)
    args = parser.parse_args()
    run_id = uuid.uuid4().hex[:12]

    banco = _resolve_banco(args)
    pipeline_name = os.getenv("UFC_PIPELINE_NAME", "ufc_lakehouse")

    garantir_tabelas_etl(banco)
    attach_db_handler(banco, run_id=run_id, origem="loader")

    previous_success = teve_sucesso_anterior(banco, pipeline_name)
    load_mode = "incremental" if previous_success else "initial_full"
    registrar_inicio(banco, pipeline_name=pipeline_name, run_id=run_id, run_dt=args.dt, load_mode=load_mode)

    log.info("=== LOADER INICIADO | run_id=%s | dt=%s | mode=%s ===", run_id, args.dt, load_mode)

    engine = _create_engine(banco)

    data_root = Path(args.data_root).resolve()
    dt = args.dt

    # Limpeza de schema solicitada: remover tabela/colunas descontinuadas.
    _drop_table_if_exists(engine, banco.silver_schema, "lutadores_detalhes")
    _drop_columns_if_exist(engine, banco.bronze_schema, "eventos", ["url_evento", "payload_json"])
    _drop_columns_if_exist(engine, banco.bronze_schema, "lutas", ["url_luta", "payload_json"])
    _drop_columns_if_exist(
        engine, banco.bronze_schema, "lutadores",
        ["url_perfil", "bio_json", "stats_json", "payload_json"],
    )
    _drop_columns_if_exist(engine, banco.silver_schema, "eventos", ["url_evento"])
    _drop_columns_if_exist(engine, banco.silver_schema, "lutas", ["url_luta", "lutadores_json"])
    _drop_columns_if_exist(
        engine, banco.silver_schema, "lutadores",
        ["url_perfil", "bio_json", "lutas_json", "stats_json"],
    )

    log.info("[BRONZE] Lendo arquivos JSONL de: %s", data_root / "bronze")
    bronze_events_src = _read_jsonl(data_root / "bronze" / "eventos" / f"dt={dt}" / "eventos.jsonl")
    bronze_fights_src = _read_jsonl(data_root / "bronze" / "lutas" / f"dt={dt}" / "lutas.jsonl")
    bronze_fighters_src = _read_jsonl(data_root / "bronze" / "lutadores" / f"dt={dt}" / "lutadores.jsonl")
    bronze_fighters_src = _enriquecer_career_stats_lutadores(bronze_fighters_src, data_root)
    log.info("[BRONZE] Lidos: %d eventos, %d lutas, %d lutadores",
             len(bronze_events_src), len(bronze_fights_src), len(bronze_fighters_src))

    bronze_events = _prepare_bronze_events(bronze_events_src, dt)
    bronze_fights = _prepare_bronze_fights(bronze_fights_src, dt)
    bronze_fighters = _prepare_bronze_fighters(bronze_fighters_src, dt)

    try:
        log.info("[BRONZE] Upsert bronze.eventos (%d linhas)...", len(bronze_events))
        _upsert_by_key(bronze_events, engine=engine, banco=banco, table_key="bronze_eventos", keys=["event_id"])

        log.info("[BRONZE] Upsert bronze.lutas (%d linhas)...", len(bronze_fights))
        _upsert_by_key(bronze_fights, engine=engine, banco=banco, table_key="bronze_lutas", keys=["fight_id"])

        log.info("[BRONZE] Upsert bronze.lutadores (%d linhas)...", len(bronze_fighters))
        _upsert_by_key(bronze_fighters, engine=engine, banco=banco, table_key="bronze_lutadores", keys=["fighter_id"])

        log.info("[SILVER] Lendo bronze do banco para construção da camada silver...")
        bronze_events_db = _read_table(engine, banco.tabela("bronze_eventos"))
        bronze_fights_db = _read_table(engine, banco.tabela("bronze_lutas"))
        bronze_fighters_db = _read_table(engine, banco.tabela("bronze_lutadores"))
        log.info("[SILVER] Bronze lido: %d eventos, %d lutas, %d lutadores",
                 len(bronze_events_db), len(bronze_fights_db), len(bronze_fighters_db))

        log.info("[MAPA_IDS] Populando etl.mapa_ids com IDs sequenciais...")
        _popular_mapa_ids(engine, bronze_events_db, bronze_fights_db, bronze_fighters_db)
        id_maps = _ler_mapa_ids(engine)
        log.info("[MAPA_IDS] Mapa lido: %d eventos, %d lutas, %d lutadores",
                 len(id_maps[0]), len(id_maps[1]), len(id_maps[2]))

        silver_events, silver_fights, silver_fighters = _build_silver_from_bronze(
            bronze_events_db, bronze_fights_db, bronze_fighters_db, id_maps
        )
        silver_lutas_lutadores = _build_silver_lutas_lutadores(silver_fights, id_maps)
        log.info(
            "[SILVER] Silver construído: %d eventos, %d lutas, %d lutadores, %d linhas lutas_lutadores",
            len(silver_events), len(silver_fights), len(silver_fighters), len(silver_lutas_lutadores)
        )

        log.info("[SILVER] Upsert silver.eventos...")
        _upsert_by_key(silver_events, engine=engine, banco=banco, table_key="silver_eventos", keys=["id_evento"])
        silver_fights_sql = silver_fights.drop(columns=["lutadores_json"], errors="ignore")
        log.info("[SILVER] Upsert silver.lutas...")
        _upsert_by_key(silver_fights_sql, engine=engine, banco=banco, table_key="silver_lutas", keys=["id_luta"])
        log.info("[SILVER] Upsert silver.lutadores...")
        _upsert_by_key(silver_fighters, engine=engine, banco=banco, table_key="silver_lutadores", keys=["id_lutador"])
        log.info("[SILVER] Upsert silver.lutas_lutadores...")
        _upsert_by_key(
            silver_lutas_lutadores,
            engine=engine,
            banco=banco,
            table_key="silver_lutas_lutadores",
            keys=["id_luta", "fighter_id_hash", "round_number"],
        )

        log.info("[GOLD] Calculando métricas gold...")
        gold_event_metrics, gold_fighter_metrics = _build_gold_metrics(
            silver_events, silver_fights, silver_fighters, id_maps
        )
        gold_event_metrics["load_dt"] = dt
        gold_fighter_metrics["load_dt"] = dt
        log.info("[GOLD] Gold calculado: %d métricas de eventos, %d métricas de lutadores",
                 len(gold_event_metrics), len(gold_fighter_metrics))

        log.info("[GOLD] Upsert gold.metricas_eventos...")
        _upsert_by_key(gold_event_metrics, engine=engine, banco=banco, table_key="gold_metricas_eventos", keys=["id_evento"])
        log.info("[GOLD] Upsert gold.metricas_lutadores...")
        _upsert_by_key(gold_fighter_metrics, engine=engine, banco=banco, table_key="gold_metricas_lutadores", keys=["id_lutador"])

        marcar_sucesso(banco, pipeline_name=pipeline_name, run_dt=dt)
        registrar_fim(banco, run_id=run_id, status="success", message=f"mode={load_mode}")
        log.info("=== LOADER CONCLUÍDO COM SUCESSO | run_id=%s | mode=%s ===", run_id, load_mode)

        print(f"Carga Bronze -> Silver -> Gold finalizada. mode={load_mode}")
    except Exception as exc:
        log.error("=== LOADER FALHOU | run_id=%s | erro: %s ===", run_id, exc, exc_info=True)
        registrar_fim(banco, run_id=run_id, status="failed", message=str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
