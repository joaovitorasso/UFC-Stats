"""
Executor de predição para uma luta individual.
Contém a lógica de orquestração compartilhada entre os scripts
prever_evento.py e prever_upcoming.py:
  - extrair lutadores do JSON
  - carregar dados + rodar todos os módulos de análise
  - formatar resultado para salvar no banco
  - salvar via MERGE (upsert por id_luta)
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from ufc_pipeline.predicao.base import _v
from ufc_pipeline.predicao.consultas import carregar_dados
from ufc_pipeline.predicao.score import calcular_score_final
from ufc_pipeline.predicao.modulos import (
    estilo, round_a_round, curva_fadiga, indice_cardio,
    timing_finalizacao, adaptacao_oponente, perfil_pressao,
    troca_dano, controle_solo, mapa_alvos, medias_apostas,
    probabilidade_metodo, tendencia,
    declinio_etario, pico_round, poder_nocaute, durabilidade,
    crescimento_intra_luta, atividade_recente,
)

TABLE_PREDICOES = "gold.predicoes_evento"

log = logging.getLogger(__name__)


# ── Extração de lutadores do JSON ─────────────────────────────────────────────

def extrair_lutadores(lutadores_json: Any) -> tuple[dict | None, dict | None]:
    if lutadores_json is None:
        return None, None
    if isinstance(lutadores_json, str):
        try:
            data = json.loads(lutadores_json)
        except json.JSONDecodeError:
            return None, None
    elif isinstance(lutadores_json, list):
        data = lutadores_json
    else:
        return None, None
    if not isinstance(data, list) or len(data) < 2:
        return None, None
    return data[0], data[1]


def _extrair_nomes_luta(fight_row: pd.Series) -> tuple[str | None, str | None]:
    """Extrai nomes dos lutadores da linha da luta.
    Prioriza colunas normalizadas (nome_lutador_a/b) e mantém fallback para JSON legado.
    """
    nome_a = str(fight_row.get("nome_lutador_a") or "").strip()
    nome_b = str(fight_row.get("nome_lutador_b") or "").strip()
    if nome_a and nome_b:
        return nome_a, nome_b

    f1, f2 = extrair_lutadores(fight_row.get("lutadores_json"))
    nome_a_json = str((f1 or {}).get("name") or "").strip()
    nome_b_json = str((f2 or {}).get("name") or "").strip()
    if nome_a_json and nome_b_json:
        return nome_a_json, nome_b_json
    return None, None


# ── Predição de uma linha de luta ─────────────────────────────────────────────

def executar_predicao_luta(engine: Any, fight_row: pd.Series, run_id: str) -> dict | None:
    """
    Executa a predição completa para uma luta.
    Retorna dict pronto para salvar em gold.predicoes_evento,
    ou None se não houver dados suficientes para um dos lutadores.
    """
    nome_a, nome_b = _extrair_nomes_luta(fight_row)
    if not nome_a or not nome_b:
        log.warning(
            "[%s] nomes dos lutadores ausentes (nem colunas normalizadas nem JSON legado).",
            fight_row.get("id_luta", "?"),
        )
        return None

    try:
        a = carregar_dados(engine, nome_a)
    except SystemExit:
        log.warning("Lutador não encontrado: '%s'. Pulando.", nome_a)
        return None

    try:
        b = carregar_dados(engine, nome_b)
    except SystemExit:
        log.warning("Lutador não encontrado: '%s'. Pulando.", nome_b)
        return None

    fid_a = int(a.stats.get("id_lutador") or 0) or None
    fid_b = int(b.stats.get("id_lutador") or 0) or None

    analises = {
        "estilo":                 estilo.analisar(a, b),
        "round_a_round":          round_a_round.analisar(a, b),
        "curva_fadiga":           curva_fadiga.analisar(a, b),
        "indice_cardio":          indice_cardio.analisar(a, b),
        "timing_finalizacao":     timing_finalizacao.analisar(a, b),
        "adaptacao_oponente":     adaptacao_oponente.analisar(a, b),
        "perfil_pressao":         perfil_pressao.analisar(a, b),
        "troca_dano":             troca_dano.analisar(a, b),
        "controle_solo":          controle_solo.analisar(a, b),
        "mapa_alvos":             mapa_alvos.analisar(a, b),
        "medias_apostas":         medias_apostas.analisar(a, b),
        "probabilidade_metodo":   probabilidade_metodo.analisar(a, b),
        "tendencia":              tendencia.analisar(a, b),
        "declinio_etario":        declinio_etario.analisar(a, b),
        "pico_round":             pico_round.analisar(a, b),
        "poder_nocaute":          poder_nocaute.analisar(a, b),
        "durabilidade":           durabilidade.analisar(a, b),
        "crescimento_intra_luta": crescimento_intra_luta.analisar(a, b),
        "atividade_recente":      atividade_recente.analisar(a, b),
    }
    score = calcular_score_final(a, b, analises)

    prob_a       = float(score.get(f"probabilidade_{a.nome}", 50.0))
    prob_b       = float(score.get(f"probabilidade_{b.nome}", 50.0))
    vencedor     = score["vencedor_previsto"]
    fid_vencedor = fid_a if vencedor == a.nome else fid_b
    total        = max(prob_a + prob_b, 0.01)

    return {
        "id_luta":             fight_row["id_luta"],
        "id_evento":           fight_row["id_evento"],
        "nome_evento":         fight_row.get("nome_evento"),
        "data_evento":         str(fight_row.get("data_evento") or ""),
        "ordem_luta":          fight_row.get("ordem_luta"),
        "tipo_luta":           fight_row.get("tipo_luta"),
        "id_lutador_a":        fid_a,
        "nome_lutador_a":      a.nome,
        "id_lutador_b":        fid_b,
        "nome_lutador_b":      b.nome,
        "probabilidade_a":     prob_a,
        "probabilidade_b":     prob_b,
        "vencedor_previsto":   vencedor,
        "id_lutador_vencedor": fid_vencedor,
        "metodo_previsto":     score["metodo_previsto"],
        "confianca":           score["confianca"],
        "score_a":             round(prob_a / 100 * total, 4),
        "score_b":             round(prob_b / 100 * total, 4),
        "estilo_a":            analises["estilo"]["estilo_a"],
        "estilo_b":            analises["estilo"]["estilo_b"],
        "indice_cardio_a":     float(_v(a.stats, "indice_cardio", 100.0)),
        "indice_cardio_b":     float(_v(b.stats, "indice_cardio", 100.0)),
        "win_rate_a":          float(_v(a.stats, "win_rate_pct")),
        "win_rate_b":          float(_v(b.stats, "win_rate_pct")),
        "predicao_em":         datetime.utcnow().isoformat() + "Z",
        "run_id":              run_id,
    }


# ── Persistência no banco ─────────────────────────────────────────────────────

def _quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _split_table(full_name: str) -> tuple[str, str]:
    parts = [p.strip().strip("[]") for p in full_name.split(".") if p.strip()]
    return (parts[-2], parts[-1]) if len(parts) >= 2 else ("dbo", parts[0])


def _table_exists(engine: Any, schema: str, table: str) -> bool:
    sql = text(
        "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :schema AND t.name = :table"
    )
    with engine.begin() as conn:
        return conn.execute(sql, {"schema": schema, "table": table}).first() is not None


def _ensure_schema(engine: Any, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("IF SCHEMA_ID(:s) IS NULL EXEC('CREATE SCHEMA ' + :s)"), {"s": schema})


def salvar_predicoes(engine: Any, rows: list[dict], table: str = TABLE_PREDICOES) -> int:
    """
    Persiste as predições em `table` via MERGE (upsert por id_luta).
    Cria a tabela automaticamente se não existir.
    Retorna a quantidade de linhas processadas.
    """
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    schema, tbl = _split_table(table)
    _ensure_schema(engine, schema)

    stg      = f"__stg_pred_{uuid.uuid4().hex[:8]}"
    tgt_full = f"{_quote_ident(schema)}.{_quote_ident(tbl)}"
    stg_full = f"{_quote_ident(schema)}.{_quote_ident(stg)}"
    cols     = list(df.columns)
    keys     = ["id_luta"]

    if not _table_exists(engine, schema, tbl):
        df.head(0).to_sql(tbl, engine, schema=schema, if_exists="replace", index=False)

    df.head(0).to_sql(stg, engine, schema=schema, if_exists="replace", index=False)

    safe_df     = df.astype(object).where(pd.notna(df), None)
    col_list    = ", ".join([_quote_ident(c) for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql  = (
        f"INSERT INTO {_quote_ident(schema)}.{_quote_ident(stg)} "
        f"({col_list}) VALUES ({placeholders})"
    )
    conn_raw = engine.raw_connection()
    try:
        cursor = conn_raw.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, [tuple(r) for r in safe_df.itertuples(index=False, name=None)])
        conn_raw.commit()
    finally:
        conn_raw.close()

    non_keys      = [c for c in cols if c not in keys]
    on_clause     = " AND ".join([f"tgt.{_quote_ident(k)} = src.{_quote_ident(k)}" for k in keys])
    update_clause = ", ".join([f"tgt.{_quote_ident(c)} = src.{_quote_ident(c)}" for c in non_keys])
    insert_cols   = ", ".join([_quote_ident(c) for c in cols])
    insert_vals   = ", ".join([f"src.{_quote_ident(c)}" for c in cols])
    merge_sql = (
        f"SET NOCOUNT ON; "
        f"MERGE {tgt_full} AS tgt "
        f"USING {stg_full} AS src ON {on_clause} "
        f"WHEN MATCHED THEN UPDATE SET {update_clause} "
        f"WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
    finally:
        with engine.begin() as conn:
            conn.execute(text(
                f"IF OBJECT_ID('{schema}.{stg}','U') IS NOT NULL DROP TABLE {stg_full};"
            ))
    return len(df)
