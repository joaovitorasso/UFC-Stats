import logging

import pandas as pd
from sqlalchemy import Integer, String, text

from layers.silver.db import _executar_sql_arquivo, _salvar_tabela
from layers.silver.transformers import _parse_lutadores_json

log = logging.getLogger(__name__)


def _descricao_bonus(codigo_bonus: int) -> str:
    mapa = {
        1: "Luta da Noite",
        2: "Performance da Noite",
        3: "Finalizacao da Noite",
        4: "Nocaute da Noite",
    }
    return mapa.get(codigo_bonus, f"Bonus {codigo_bonus}")


def _garantir_dim_bonus(engine, schema: str) -> None:
    _executar_sql_arquivo(engine, "dim_bonus.sql", schema)


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
    _executar_sql_arquivo(engine, "dim_evento.sql", schema)


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
    _executar_sql_arquivo(engine, "dim_luta.sql", schema)


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
    _executar_sql_arquivo(engine, "dim_lutador.sql", schema)


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
