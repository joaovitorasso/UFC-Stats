import argparse
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Integer, String, create_engine, text

from banco import ConexaoBanco, montar_conn_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _listar_arquivos_particao(base_dir: Path, nome_arquivo: str, dt: str | None) -> list[tuple[Path, str]]:
    arquivos: list[tuple[Path, str]] = []
    if dt:
        arquivo = base_dir / f"dt={dt}" / nome_arquivo
        if not arquivo.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {arquivo}")
        return [(arquivo, dt)]

    for particao in sorted(base_dir.glob("dt=*")):
        if not particao.is_dir():
            continue
        dt_valor = particao.name.replace("dt=", "")
        arquivo = particao / nome_arquivo
        if arquivo.exists():
            arquivos.append((arquivo, dt_valor))

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {base_dir}")
    return arquivos


def _ler_jsonl_particionado(base_dir: Path, nome_arquivo: str, dt: str | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for arquivo, dt_valor in _listar_arquivos_particao(base_dir, nome_arquivo, dt):
        df = pd.read_json(arquivo, lines=True)
        df["dt_particao"] = dt_valor
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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

    return df.rename(
        columns={
            "event_name": "nome_evento",
            "bout_order": "ordem_luta",
            "bonus_code": "codigo_bonus",
            "bout": "tipo_luta",
            "method": "metodo",
            "time": "tempo",
            "time_format": "formato_tempo",
            "referee": "arbitro",
            "ingested_at": "ingerido_em",
        }
    )[
        [
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
    ]


def _preparar_silver_lutadores(df: pd.DataFrame) -> pd.DataFrame:
    df = _dedupe_por_chave(df, "fighter_id")
    df = df.copy()

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


def _descricao_bonus(codigo_bonus: int) -> str:
    mapa = {
        1: "Luta da Noite",
        2: "Performance da Noite",
        3: "Finalizacao da Noite",
        4: "Nocaute da Noite",
    }
    return mapa.get(codigo_bonus, f"Bonus {codigo_bonus}")


def _garantir_dim_bonus(engine, schema: str) -> None:
    sql = f"""
IF OBJECT_ID(N'[{schema}].[dim_bonus]', N'U') IS NULL
BEGIN
    CREATE TABLE [{schema}].[dim_bonus] (
        codigo_bonus INT NOT NULL PRIMARY KEY,
        descricao_bonus VARCHAR(120) NOT NULL,
        criado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def _carregar_dim_bonus(engine, schema: str) -> pd.DataFrame:
    query = text(f"SELECT codigo_bonus, descricao_bonus FROM [{schema}].[dim_bonus]")
    return pd.read_sql_query(query, engine)


def _upsert_dim_bonus(engine, schema: str, silver_lutas: pd.DataFrame) -> None:
    codigos_dados: set[int] = set()
    if "codigo_bonus" in silver_lutas.columns:
        for valor in silver_lutas["codigo_bonus"]:
            if pd.isna(valor):
                continue
            try:
                codigos_dados.add(int(valor))
            except Exception:
                continue

    codigos = sorted(set([1, 2, 3, 4]).union(codigos_dados))
    if not codigos:
        return

    candidatos = pd.DataFrame(
        [{"codigo_bonus": c, "descricao_bonus": _descricao_bonus(c)} for c in codigos]
    )

    atual = _carregar_dim_bonus(engine, schema)
    if atual.empty:
        faltantes = candidatos.copy()
    else:
        faltantes = candidatos[~candidatos["codigo_bonus"].isin(atual["codigo_bonus"])].copy()

    if not faltantes.empty:
        faltantes.to_sql(
            name="dim_bonus",
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={"codigo_bonus": Integer(), "descricao_bonus": String(120)},
        )

    atualizada = _carregar_dim_bonus(engine, schema)
    base_desc = candidatos.set_index("codigo_bonus")["descricao_bonus"].to_dict()
    with engine.begin() as conn:
        for row in atualizada.itertuples(index=False):
            desc_nova = base_desc.get(int(row.codigo_bonus))
            if not desc_nova:
                continue
            if (row.descricao_bonus or "").strip() == desc_nova.strip():
                continue
            conn.execute(
                text(
                    f"UPDATE [{schema}].[dim_bonus] "
                    "SET descricao_bonus = :descricao_bonus, atualizado_em = SYSUTCDATETIME() "
                    "WHERE codigo_bonus = :codigo_bonus"
                ),
                {"descricao_bonus": desc_nova, "codigo_bonus": int(row.codigo_bonus)},
            )


def _garantir_dim_evento(engine, schema: str) -> None:
    sql = f"""
IF OBJECT_ID(N'[{schema}].[dim_evento]', N'U') IS NULL
BEGIN
    CREATE TABLE [{schema}].[dim_evento] (
        id_evento INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        event_id VARCHAR(40) NOT NULL UNIQUE,
        nome_evento VARCHAR(300) NULL,
        event_url VARCHAR(300) NULL,
        criado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF COL_LENGTH(N'[{schema}].[dim_evento]', N'event_url') IS NULL
BEGIN
    ALTER TABLE [{schema}].[dim_evento] ADD event_url VARCHAR(300) NULL;
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def _carregar_mapa_dim_evento(engine, schema: str) -> pd.DataFrame:
    query = text(f"SELECT id_evento, event_id, nome_evento, event_url FROM [{schema}].[dim_evento]")
    return pd.read_sql_query(query, engine)


def _upsert_dim_evento(engine, schema: str, silver_eventos: pd.DataFrame) -> pd.DataFrame:
    candidatos = (
        silver_eventos[["event_id", "nome", "event_url"]]
        .dropna(subset=["event_id"])
        .drop_duplicates(subset=["event_id"])
        .rename(columns={"nome": "nome_evento"})
    )

    dim_atual = _carregar_mapa_dim_evento(engine, schema)
    if dim_atual.empty:
        faltantes = candidatos.copy()
    else:
        faltantes = candidatos[~candidatos["event_id"].isin(dim_atual["event_id"])].copy()

    if not faltantes.empty:
        faltantes.to_sql(
            name="dim_evento",
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={"event_id": String(40), "nome_evento": String(300), "event_url": String(300)},
        )

    dim_novo = _carregar_mapa_dim_evento(engine, schema)
    base = candidatos.set_index("event_id").to_dict(orient="index")
    with engine.begin() as conn:
        for row in dim_novo.itertuples(index=False):
            atual = base.get(row.event_id)
            if not atual:
                continue
            nome_novo = atual.get("nome_evento")
            url_nova = atual.get("event_url")
            nome_antigo = row.nome_evento
            url_antiga = row.event_url

            mudou_nome = bool(nome_novo) and (nome_antigo or "").strip() != str(nome_novo).strip()
            mudou_url = bool(url_nova) and (url_antiga or "") != str(url_nova)
            if not mudou_nome and not mudou_url:
                continue
            conn.execute(
                text(
                    f"UPDATE [{schema}].[dim_evento] "
                    "SET nome_evento = COALESCE(:nome, nome_evento), "
                    "event_url = COALESCE(:event_url, event_url), "
                    "atualizado_em = SYSUTCDATETIME() "
                    "WHERE event_id = :event_id"
                ),
                {"nome": nome_novo, "event_url": url_nova, "event_id": row.event_id},
            )

    return _carregar_mapa_dim_evento(engine, schema)[["id_evento", "event_id"]]


def _garantir_dim_luta(engine, schema: str) -> None:
    sql = f"""
IF OBJECT_ID(N'[{schema}].[dim_luta]', N'U') IS NULL
BEGIN
    CREATE TABLE [{schema}].[dim_luta] (
        id_luta INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fight_id VARCHAR(40) NOT NULL UNIQUE,
        id_evento INT NULL,
        fight_url VARCHAR(300) NULL,
        nome_evento VARCHAR(300) NULL,
        criado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def _carregar_mapa_dim_luta(engine, schema: str) -> pd.DataFrame:
    query = text(f"SELECT id_luta, fight_id, id_evento, fight_url FROM [{schema}].[dim_luta]")
    return pd.read_sql_query(query, engine)


def _upsert_dim_luta(
    engine,
    schema: str,
    silver_lutas: pd.DataFrame,
    mapa_dim_evento: pd.DataFrame,
) -> pd.DataFrame:
    mapa_evento = mapa_dim_evento.set_index("event_id")["id_evento"].to_dict()

    candidatos = (
        silver_lutas[["fight_id", "event_id", "fight_url", "nome_evento"]]
        .dropna(subset=["fight_id"])
        .drop_duplicates(subset=["fight_id"])
        .copy()
    )
    candidatos["id_evento"] = candidatos["event_id"].map(mapa_evento).astype("Int64")
    candidatos = candidatos.drop(columns=["event_id"])

    dim_atual = _carregar_mapa_dim_luta(engine, schema)
    if dim_atual.empty:
        faltantes = candidatos.copy()
    else:
        faltantes = candidatos[~candidatos["fight_id"].isin(dim_atual["fight_id"])].copy()

    if not faltantes.empty:
        faltantes["id_evento"] = faltantes["id_evento"].apply(lambda x: None if pd.isna(x) else int(x))
        faltantes.to_sql(
            name="dim_luta",
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={
                "fight_id": String(40),
                "id_evento": Integer(),
                "fight_url": String(300),
                "nome_evento": String(300),
            },
        )

    dim_novo = _carregar_mapa_dim_luta(engine, schema)
    base = candidatos.set_index("fight_id").to_dict(orient="index")
    with engine.begin() as conn:
        for row in dim_novo.itertuples(index=False):
            atual = base.get(row.fight_id)
            if not atual:
                continue
            novo_id_evento = atual.get("id_evento")
            novo_fight_url = atual.get("fight_url")
            novo_nome = atual.get("nome_evento")
            conn.execute(
                text(
                    f"UPDATE [{schema}].[dim_luta] "
                    "SET id_evento = :id_evento, fight_url = :fight_url, nome_evento = :nome_evento, "
                    "atualizado_em = SYSUTCDATETIME() "
                    "WHERE fight_id = :fight_id"
                ),
                {
                    "id_evento": None if pd.isna(novo_id_evento) else int(novo_id_evento),
                    "fight_url": novo_fight_url,
                    "nome_evento": novo_nome,
                    "fight_id": row.fight_id,
                },
            )

    return _carregar_mapa_dim_luta(engine, schema)[["id_luta", "fight_id", "id_evento", "fight_url"]]


def _garantir_dim_lutador(engine, schema: str) -> None:
    sql = f"""
IF OBJECT_ID(N'[{schema}].[dim_lutador]', N'U') IS NULL
BEGIN
    CREATE TABLE [{schema}].[dim_lutador] (
        id_lutador INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fighter_id VARCHAR(40) NOT NULL UNIQUE,
        nome_lutador VARCHAR(200) NULL,
        url_perfil VARCHAR(300) NULL,
        criado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF COL_LENGTH(N'[{schema}].[dim_lutador]', N'url_perfil') IS NULL
BEGIN
    ALTER TABLE [{schema}].[dim_lutador] ADD url_perfil VARCHAR(300) NULL;
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def _carregar_mapa_dim_lutador(engine, schema: str) -> pd.DataFrame:
    query = text(f"SELECT id_lutador, fighter_id, nome_lutador, url_perfil FROM [{schema}].[dim_lutador]")
    return pd.read_sql_query(query, engine)


def _candidatos_lutador_de_lutas(silver_lutas: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, luta in silver_lutas.iterrows():
        for lutador in _parse_lutadores_json(luta.get("lutadores_json")):
            fid = lutador.get("fighter_id")
            if not fid:
                continue
            rows.append(
                {
                    "fighter_id": fid,
                    "nome_lutador": lutador.get("name"),
                    "url_perfil": lutador.get("profile_url"),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["fighter_id", "nome_lutador", "url_perfil"])
    return pd.DataFrame(rows).drop_duplicates(subset=["fighter_id"])


def _upsert_dim_lutador(engine, schema: str, silver_lutadores: pd.DataFrame, silver_lutas: pd.DataFrame) -> pd.DataFrame:
    candidatos_perfil = (
        silver_lutadores[["fighter_id", "nome", "url_perfil"]]
        .dropna(subset=["fighter_id"])
        .drop_duplicates(subset=["fighter_id"])
        .rename(columns={"nome": "nome_lutador"})
    )
    candidatos_luta = _candidatos_lutador_de_lutas(silver_lutas)
    candidatos = pd.concat([candidatos_perfil, candidatos_luta], ignore_index=True)
    candidatos = (
        candidatos.groupby("fighter_id", as_index=False)
        .agg(
            nome_lutador=("nome_lutador", "first"),
            url_perfil=("url_perfil", "first"),
        )
    )

    dim_atual = _carregar_mapa_dim_lutador(engine, schema)

    if dim_atual.empty:
        faltantes = candidatos.copy()
    else:
        faltantes = candidatos[~candidatos["fighter_id"].isin(dim_atual["fighter_id"])].copy()

    if not faltantes.empty:
        faltantes.to_sql(
            name="dim_lutador",
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={"fighter_id": String(40), "nome_lutador": String(200), "url_perfil": String(300)},
        )

    dim_novo = _carregar_mapa_dim_lutador(engine, schema)

    base = candidatos.set_index("fighter_id").to_dict(orient="index")
    with engine.begin() as conn:
        for row in dim_novo.itertuples(index=False):
            atual = base.get(row.fighter_id)
            if not atual:
                continue
            nome_novo = atual.get("nome_lutador")
            url_nova = atual.get("url_perfil")
            nome_antigo = row.nome_lutador
            url_antiga = row.url_perfil

            mudou_nome = bool(nome_novo) and (nome_antigo or "").strip() != str(nome_novo).strip()
            mudou_url = bool(url_nova) and (url_antiga or "") != str(url_nova)
            if not mudou_nome and not mudou_url:
                continue
            conn.execute(
                text(
                    f"UPDATE [{schema}].[dim_lutador] "
                    "SET nome_lutador = COALESCE(:nome, nome_lutador), "
                    "url_perfil = COALESCE(:url_perfil, url_perfil), "
                    "atualizado_em = SYSUTCDATETIME() "
                    "WHERE fighter_id = :fighter_id"
                ),
                {"nome": nome_novo, "url_perfil": url_nova, "fighter_id": row.fighter_id},
            )

    return _carregar_mapa_dim_lutador(engine, schema)[["id_lutador", "fighter_id"]]


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


def _criar_engine(banco: ConexaoBanco):
    conn_str = montar_conn_str(banco)
    if not conn_str:
        raise RuntimeError("Nao foi possivel montar a conexao ODBC. Verifique driver e .env.")
    # Evita truncamento intermitente no pyodbc executemany quando os tamanhos
    # de string variam entre linhas (caso comum em colunas textuais de historico).
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")


def _garantir_schema(engine, schema: str) -> None:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema):
        raise ValueError(f"Schema invalido: {schema}")
    sql = text(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")
    with engine.begin() as conn:
        conn.execute(sql)


def _salvar_tabela(df: pd.DataFrame, engine, schema: str, tabela: str) -> None:
    log.info("Gravando %s.%s (%d linhas)...", schema, tabela, len(df))
    df_sql = df.copy()

    # Evita inferência problemática de TIMESTAMP/ROWVERSION no SQL Server.
    for col in df_sql.columns:
        if pd.api.types.is_datetime64_any_dtype(df_sql[col]):
            df_sql[col] = df_sql[col].astype("string")
            continue

        # Algumas colunas chegam como "object" com Timestamps dentro.
        serie = df_sql[col]
        if not pd.api.types.is_object_dtype(serie):
            continue
        valores = [v for v in serie.tolist() if v is not None and not (isinstance(v, float) and pd.isna(v))]
        if valores and any(isinstance(v, (datetime, pd.Timestamp)) for v in valores[:50]):
            df_sql[col] = serie.apply(lambda x: None if pd.isna(x) else str(x))

    for col_data in ["ingerido_em", "dt_particao"]:
        if col_data in df_sql.columns:
            df_sql[col_data] = df_sql[col_data].apply(_formatar_data_ddmmaaaa)

    dtype_override = {}
    if "ingerido_em" in df_sql.columns:
        dtype_override["ingerido_em"] = String(10)
    if "data_evento" in df_sql.columns:
        dtype_override["data_evento"] = String(10)
    if "dt_particao" in df_sql.columns:
        dtype_override["dt_particao"] = String(10)
    if "data_nascimento" in df_sql.columns:
        dtype_override["data_nascimento"] = String(40)
    if "id_lutador" in df_sql.columns:
        dtype_override["id_lutador"] = Integer()
    if "id_evento" in df_sql.columns:
        dtype_override["id_evento"] = Integer()
    if "id_luta" in df_sql.columns:
        dtype_override["id_luta"] = Integer()

    for id_col in ["id_lutador", "id_evento", "id_luta"]:
        if id_col in df_sql.columns:
            df_sql[id_col] = df_sql[id_col].apply(lambda x: None if pd.isna(x) else int(x))

    if tabela in {"historico_lutas", "historico_lutador"}:
        dtype_override.update(
            {
                "id_lutador": Integer(),
                "id_luta": Integer(),
                "id_evento": Integer(),
                "nome_lutador": String(200),
                "resultado": String(20),
                "nome_evento": String(300),
                "ordem_luta": String(20),
                "codigo_bonus": Integer(),
                "tipo_luta": String(40),
                "metodo": String(120),
                "round_final": String(20),
                "round_num": Integer(),
                "tempo": String(20),
                "formato_tempo": String(80),
                "arbitro": String(120),
                "luta_titulo": Integer(),
                "sig_str": String(40),
                "sig_str_pct": String(20),
                "head": String(40),
                "body": String(40),
                "leg": String(40),
                "distance": String(40),
                "clinch": String(40),
                "ground": String(40),
                "ingerido_em": String(10),
                "dt_particao": String(10),
            }
        )
        if "luta_titulo" in df_sql.columns:
            df_sql["luta_titulo"] = df_sql["luta_titulo"].fillna(False).astype(bool).astype(int)

    df_sql.to_sql(
        name=tabela,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype=dtype_override,
    )


def carregar_silver(data_root: Path, dt: str | None = None) -> tuple[int, int, int, int]:
    bronze_root = data_root / "bronze"

    log.info("Lendo bronze de: %s", bronze_root)
    bronze_eventos = _ler_jsonl_particionado(bronze_root / "eventos", "eventos.jsonl", dt)
    bronze_lutas = _ler_jsonl_particionado(bronze_root / "lutas", "lutas.jsonl", dt)
    bronze_lutadores = _ler_jsonl_particionado(bronze_root / "lutadores", "lutadores.jsonl", dt)

    silver_eventos = _preparar_silver_eventos(bronze_eventos)
    silver_lutas = _preparar_silver_lutas(bronze_lutas)
    silver_lutadores = _preparar_silver_lutadores(bronze_lutadores)

    banco = ConexaoBanco.do_env()
    engine = _criar_engine(banco)

    try:
        _garantir_schema(engine, banco.silver_schema)
        with engine.begin() as conn:
            conn.execute(text(f"IF OBJECT_ID('[{banco.silver_schema}].[lutadores_lutas]', 'U') IS NOT NULL DROP TABLE [{banco.silver_schema}].[lutadores_lutas]"))
            conn.execute(text(f"IF OBJECT_ID('[{banco.silver_schema}].[historico_lutador]', 'U') IS NOT NULL DROP TABLE [{banco.silver_schema}].[historico_lutador]"))
        _garantir_dim_bonus(engine, banco.silver_schema)
        _garantir_dim_evento(engine, banco.silver_schema)
        _garantir_dim_luta(engine, banco.silver_schema)
        _garantir_dim_lutador(engine, banco.silver_schema)

        _upsert_dim_bonus(engine, banco.silver_schema, silver_lutas)
        mapa_dim_evento = _upsert_dim_evento(engine, banco.silver_schema, silver_eventos)
        mapa_dim_luta = _upsert_dim_luta(engine, banco.silver_schema, silver_lutas, mapa_dim_evento)
        mapa_dim_lutador = _upsert_dim_lutador(engine, banco.silver_schema, silver_lutadores, silver_lutas)

        silver_eventos_ids = _aplicar_id_evento(silver_eventos, mapa_dim_evento)
        silver_lutas_ids = _aplicar_ids_luta(silver_lutas, mapa_dim_evento, mapa_dim_luta)
        silver_lutadores_ids = _aplicar_id_lutador(silver_lutadores, mapa_dim_lutador)
        silver_historico_lutas = _preparar_silver_historico_lutador(
            silver_lutas_ids,
            mapa_dim_lutador,
            mapa_dim_luta,
        )

        silver_eventos_out = silver_eventos_ids.drop(columns=["event_id"], errors="ignore")
        silver_eventos_out = silver_eventos_out.drop(columns=["event_url"], errors="ignore")
        silver_lutas_out = silver_lutas_ids.drop(
            columns=["fight_id", "event_id", "fight_url", "lutadores_json"],
            errors="ignore",
        )
        silver_lutadores_out = silver_lutadores_ids.drop(columns=["fighter_id", "url_perfil"], errors="ignore")

        _salvar_tabela(silver_eventos_out, engine, banco.silver_schema, "eventos")
        _salvar_tabela(silver_lutas_out, engine, banco.silver_schema, "lutas")
        _salvar_tabela(silver_lutadores_out, engine, banco.silver_schema, "lutadores")
        _salvar_tabela(silver_historico_lutas, engine, banco.silver_schema, "historico_lutas")
    finally:
        engine.dispose()

    return len(silver_eventos), len(silver_lutas), len(silver_lutadores), len(silver_historico_lutas)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)

    parser = argparse.ArgumentParser(description="Carga simples de bronze (arquivos) para silver (SQL Server).")
    parser.add_argument("--dt", default=None, help="Particao dt=YYYY-MM-DD. Se vazio, usa todas as particoes.")
    parser.add_argument("--data-root", default="./data", help="Pasta base de dados locais.")
    args = parser.parse_args()

    total_eventos, total_lutas, total_lutadores, total_historico_lutas = carregar_silver(
        Path(args.data_root).resolve(), args.dt
    )

    log.info("Carga silver finalizada com sucesso.")
    print(
        f"Silver carregada: eventos={total_eventos}, "
        f"lutas={total_lutas}, lutadores={total_lutadores}, historico_lutas={total_historico_lutas}"
    )


if __name__ == "__main__":
    main()
