import json
import re

import pandas as pd


def _dedupe_por_chave(df: pd.DataFrame, chave: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    df = df.copy()
    if "ingested_at" in df.columns:
        df["_ingested"] = pd.to_datetime(df["ingested_at"], errors="coerce")
        df = df.sort_values([chave, "_ingested"], ascending=[True, False])
        df = df.drop(columns=["_ingested"])
    df = df.drop_duplicates(subset=[chave], keep="first")
    return df.reset_index(drop=True)


def _to_json_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if value is pd.NA:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _dict_value(value: object, key: str) -> object:
    if isinstance(value, dict):
        return value.get(key)
    return None


def _valor_texto(value: object) -> str | None:
    if value is None:
        return None
    if value is pd.NA:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    txt = str(value).strip()
    return txt or None


def _altura_para_cm(value: object) -> int | None:
    txt = _valor_texto(value)
    if not txt:
        return None

    m_ft_in = re.search(r"(\d+)\s*'\s*(\d+)", txt)
    if m_ft_in:
        total_polegadas = int(m_ft_in.group(1)) * 12 + int(m_ft_in.group(2))
        return int(round(total_polegadas * 2.54))

    m_cm = re.search(r"(\d+(?:[.,]\d+)?)\s*cm", txt.lower())
    if m_cm:
        return int(round(float(m_cm.group(1).replace(",", "."))))
    return None


def _peso_para_kg(value: object) -> float | None:
    txt = _valor_texto(value)
    if not txt:
        return None

    m_lbs = re.search(r"(\d+(?:[.,]\d+)?)\s*lbs?", txt.lower())
    if m_lbs:
        lbs = float(m_lbs.group(1).replace(",", "."))
        return round(lbs * 0.45359237, 1)

    m_kg = re.search(r"(\d+(?:[.,]\d+)?)\s*kg", txt.lower())
    if m_kg:
        return round(float(m_kg.group(1).replace(",", ".")), 1)
    return None


def _alcance_para_cm(value: object) -> int | None:
    txt = _valor_texto(value)
    if not txt:
        return None

    m_pol = re.search(r"(\d+(?:[.,]\d+)?)\s*\"", txt)
    if m_pol:
        pol = float(m_pol.group(1).replace(",", "."))
        return int(round(pol * 2.54))

    m_cm = re.search(r"(\d+(?:[.,]\d+)?)\s*cm", txt.lower())
    if m_cm:
        return int(round(float(m_cm.group(1).replace(",", "."))))
    return None


def _traduzir_stance(value: object) -> str | None:
    txt = _valor_texto(value)
    if not txt:
        return None
    mapa = {
        "orthodox": "Destro",
        "southpaw": "Canhoto",
        "switch": "Ambidestro",
    }
    return mapa.get(txt.lower())


def _formatar_data_ddmmaaaa(value: object) -> str | None:
    txt = _valor_texto(value)
    if not txt:
        return None
    if re.match(r"^\d{2}/\d{2}/\d{4}$", txt):
        dt = pd.to_datetime(txt, format="%d/%m/%Y", errors="coerce")
    elif re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        dt = pd.to_datetime(txt, format="%Y-%m-%d", errors="coerce")
    else:
        dt = pd.to_datetime(txt, errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(txt, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.strftime("%d/%m/%Y")


def _formatar_data_nascimento(value: object) -> str | None:
    return _formatar_data_ddmmaaaa(value)


def _preparar_silver_eventos(df: pd.DataFrame) -> pd.DataFrame:
    df = _dedupe_por_chave(df, "event_id")
    return df.rename(
        columns={
            "name": "nome",
            "event_date": "data_evento",
            "location": "local",
            "ingested_at": "ingerido_em",
        }
    )[
        [
            "event_id",
            "event_url",
            "nome",
            "data_evento",
            "local",
            "status",
            "ingerido_em",
            "dt_particao",
        ]
    ]


def _preparar_silver_lutas(df: pd.DataFrame) -> pd.DataFrame:
    df = _dedupe_por_chave(df, "fight_id")
    df = df.copy()
    df["lutadores_json"] = df["fighters"].apply(_to_json_text)

    df_out = df.rename(
        columns={
            "event_name": "nome_evento",
            "bout_order": "ordem_luta",
            "bonus_code": "codigo_bonus",
            "bout": "tipo_luta",
            "method": "metodo",
            "time": "tempo",
            "time_format": "formato_tempo",
            "referee": "arbitro",
            "event_status": "status_evento",
            "ingested_at": "ingerido_em",
        }
    )
    colunas = [
        "fight_id",
        "event_id",
        "fight_url",
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
        "dt_particao",
    ]
    if "status_evento" in df_out.columns:
        colunas.insert(colunas.index("nome_evento") + 1, "status_evento")
    return df_out[colunas]


def _preparar_silver_lutadores(df: pd.DataFrame) -> pd.DataFrame:
    df = _dedupe_por_chave(df, "fighter_id")
    df = df.copy()

    for col in ["fighter_id", "name", "profile_url", "ingested_at", "dt_particao", "bio", "career_stats"]:
        if col not in df.columns:
            df[col] = None

    df["cartel"] = df["bio"].apply(lambda v: _dict_value(v, "record"))
    df["altura"] = df["bio"].apply(lambda v: _altura_para_cm(_dict_value(v, "height"))).astype("Int64")
    df["peso"] = df["bio"].apply(lambda v: _peso_para_kg(_dict_value(v, "weight")))
    df["alcance"] = df["bio"].apply(lambda v: _alcance_para_cm(_dict_value(v, "reach"))).astype("Int64")
    df["stance"] = df["bio"].apply(lambda v: _traduzir_stance(_dict_value(v, "stance")))
    df["data_nascimento"] = df["bio"].apply(lambda v: _formatar_data_nascimento(_dict_value(v, "dob")))

    df["slpm"] = df["career_stats"].apply(lambda v: _dict_value(v, "slpm"))
    df["str_acc_pct"] = df["career_stats"].apply(lambda v: _dict_value(v, "str_acc_pct"))
    df["sapm"] = df["career_stats"].apply(lambda v: _dict_value(v, "sapm"))
    df["str_def_pct"] = df["career_stats"].apply(lambda v: _dict_value(v, "str_def_pct"))
    df["td_avg_15min"] = df["career_stats"].apply(lambda v: _dict_value(v, "td_avg_15min"))
    df["td_acc_pct"] = df["career_stats"].apply(lambda v: _dict_value(v, "td_acc_pct"))
    df["td_def_pct"] = df["career_stats"].apply(lambda v: _dict_value(v, "td_def_pct"))
    df["sub_avg_15min"] = df["career_stats"].apply(lambda v: _dict_value(v, "sub_avg_15min"))

    return df.rename(
        columns={
            "name": "nome",
            "profile_url": "url_perfil",
            "ingested_at": "ingerido_em",
        }
    )[
        [
            "fighter_id",
            "nome",
            "url_perfil",
            "cartel",
            "altura",
            "peso",
            "alcance",
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
            "ingerido_em",
            "dt_particao",
        ]
    ]


def _parse_lutadores_json(value: object) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


def _preparar_silver_historico_lutador(
    df_lutas: pd.DataFrame,
    mapa_dim_lutador: pd.DataFrame,
    mapa_dim_luta: pd.DataFrame,
) -> pd.DataFrame:
    colunas = [
        "id_lutador",
        "id_luta",
        "id_evento",
        "nome_lutador",
        "resultado",
        "nome_evento",
        "ordem_luta",
        "codigo_bonus",
        "tipo_luta",
        "metodo",
        "round_final",
        "round_num",
        "tempo",
        "formato_tempo",
        "arbitro",
        "luta_titulo",
        "sig_str",
        "sig_str_pct",
        "head",
        "body",
        "leg",
        "distance",
        "clinch",
        "ground",
        "ingerido_em",
        "dt_particao",
    ]

    mapa_lutador = mapa_dim_lutador.set_index("fighter_id")["id_lutador"].to_dict()
    mapa_luta = mapa_dim_luta.set_index("fight_id")

    rows: list[dict] = []
    for _, luta in df_lutas.iterrows():
        fight_id = luta.get("fight_id")
        if not fight_id or fight_id not in mapa_luta.index:
            continue

        id_luta = mapa_luta.loc[fight_id, "id_luta"]
        id_evento = mapa_luta.loc[fight_id, "id_evento"]
        lutadores = _parse_lutadores_json(luta.get("lutadores_json"))
        if not lutadores:
            continue

        for pessoa in lutadores:
            fighter_id = pessoa.get("fighter_id")
            id_lutador = mapa_lutador.get(fighter_id) if fighter_id else None
            rounds_map = pessoa.get("rounds_sig_strikes") or {}
            if not isinstance(rounds_map, dict):
                rounds_map = {}

            round_keys = list(rounds_map.keys())

            def _round_sort_key(x: object) -> tuple[int, int | str]:
                txt = str(x)
                if txt.isdigit():
                    return (0, int(txt))
                return (1, txt)

            if round_keys:
                round_keys = sorted(round_keys, key=_round_sort_key)
            else:
                try:
                    total_rounds = int(str(luta.get("round") or "").strip())
                except Exception:
                    total_rounds = 0
                if total_rounds > 0:
                    round_keys = [str(i) for i in range(1, total_rounds + 1)]
                else:
                    round_keys = [None]

            for round_key in round_keys:
                stats = rounds_map.get(str(round_key), {}) if round_key is not None else {}
                if not isinstance(stats, dict):
                    stats = {}
                round_num = int(round_key) if isinstance(round_key, str) and round_key.isdigit() else None

                rows.append(
                    {
                        "id_lutador": id_lutador,
                        "id_luta": id_luta,
                        "id_evento": id_evento,
                        "nome_lutador": pessoa.get("name"),
                        "resultado": pessoa.get("result"),
                        "nome_evento": luta.get("nome_evento"),
                        "ordem_luta": luta.get("ordem_luta"),
                        "codigo_bonus": luta.get("codigo_bonus"),
                        "tipo_luta": luta.get("tipo_luta"),
                        "metodo": luta.get("metodo"),
                        "round_final": luta.get("round"),
                        "round_num": round_num,
                        "tempo": luta.get("tempo"),
                        "formato_tempo": luta.get("formato_tempo"),
                        "arbitro": luta.get("arbitro"),
                        "luta_titulo": 1 if str(luta.get("tipo_luta") or "").lower() == "title bout" else 0,
                        "sig_str": stats.get("sig_str"),
                        "sig_str_pct": stats.get("sig_str_pct"),
                        "head": stats.get("head"),
                        "body": stats.get("body"),
                        "leg": stats.get("leg"),
                        "distance": stats.get("distance"),
                        "clinch": stats.get("clinch"),
                        "ground": stats.get("ground"),
                        "ingerido_em": luta.get("ingerido_em"),
                        "dt_particao": luta.get("dt_particao"),
                    }
                )

    if not rows:
        return pd.DataFrame(columns=colunas)
    out = pd.DataFrame(rows, columns=colunas)
    out["id_lutador"] = out["id_lutador"].astype("Int64")
    out["id_luta"] = out["id_luta"].astype("Int64")
    out["id_evento"] = out["id_evento"].astype("Int64")
    return out


def _aplicar_id_lutador(silver_lutadores: pd.DataFrame, mapa_dim_lutador: pd.DataFrame) -> pd.DataFrame:
    out = silver_lutadores.merge(mapa_dim_lutador, on="fighter_id", how="left")
    out["id_lutador"] = out["id_lutador"].astype("Int64")

    colunas = ["id_lutador"] + [c for c in out.columns if c != "id_lutador"]
    return out[colunas]


def _aplicar_id_evento(silver_eventos: pd.DataFrame, mapa_dim_evento: pd.DataFrame) -> pd.DataFrame:
    out = silver_eventos.merge(mapa_dim_evento, on="event_id", how="left")
    out["id_evento"] = out["id_evento"].astype("Int64")

    colunas = ["id_evento"] + [c for c in out.columns if c != "id_evento"]
    return out[colunas]


def _aplicar_ids_luta(
    silver_lutas: pd.DataFrame,
    mapa_dim_evento: pd.DataFrame,
    mapa_dim_luta: pd.DataFrame,
) -> pd.DataFrame:
    out = silver_lutas.merge(mapa_dim_evento, on="event_id", how="left")
    out = out.merge(mapa_dim_luta[["id_luta", "fight_id"]], on="fight_id", how="left")
    out["id_evento"] = out["id_evento"].astype("Int64")
    out["id_luta"] = out["id_luta"].astype("Int64")

    colunas = ["id_luta", "id_evento"] + [c for c in out.columns if c not in {"id_luta", "id_evento"}]
    return out[colunas]
