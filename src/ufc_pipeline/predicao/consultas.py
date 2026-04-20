"""
Consultas ao banco de dados para carregar dados de lutadores.
Todas as queries filtram por id_lutador (inteiro) — nunca por hash.
"""
from __future__ import annotations

import sys
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from ufc_pipeline.banco import ConexaoBanco, montar_conn_str  # type: ignore
from ufc_pipeline.predicao.base import DadosLutador


def criar_engine(banco: ConexaoBanco):
    conn_str = montar_conn_str(banco)
    if not conn_str:
        print("[ERRO] Driver ODBC não encontrado. Instale 'ODBC Driver 17/18 for SQL Server'.")
        sys.exit(1)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        fast_executemany=True,
    )


def _buscar_lutador(engine: Any, nome: str) -> pd.DataFrame:
    sql = text("""
        SELECT * FROM gold.v_estatisticas_lutador
        WHERE nome_lutador LIKE :nome
        ORDER BY (vitorias + derrotas) DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"nome": f"%{nome}%"})


def _buscar_curva_fadiga(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT round_number, media_golpes, media_precisao,
               media_head, media_body, media_leg,
               media_distancia, media_clinch, media_ground,
               rounds_disputados
        FROM gold.v_curva_fadiga
        WHERE id_lutador = :fid
        ORDER BY round_number
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def _buscar_troca_dano(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT round_number,
               AVG(CAST(golpes_dados AS FLOAT))     AS media_dados,
               AVG(CAST(golpes_recebidos AS FLOAT)) AS media_recebidos,
               AVG(razao_troca)                     AS media_razao,
               AVG(CAST(saldo_golpes AS FLOAT))     AS media_saldo,
               COUNT(DISTINCT id_luta)              AS lutas
        FROM gold.v_troca_dano
        WHERE id_lutador = :fid
        GROUP BY round_number
        ORDER BY round_number
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def _buscar_timing_finalizacao(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT metodo, round_fim, tempo_fim, minutos_decorridos, quantidade
        FROM gold.v_timing_finalizacao
        WHERE id_lutador = :fid
        ORDER BY quantidade DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def _buscar_mapa_alvos(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT round_number,
               total_head, total_body, total_leg, total_sig,
               pct_head, pct_body, pct_leg
        FROM gold.v_mapa_alvos
        WHERE id_lutador = :fid
        ORDER BY round_number
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def _buscar_prob_metodo(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT total_lutas, total_vitorias, total_derrotas,
               v_ko_tko, v_sub, v_decisao,
               d_ko_tko, d_sub, d_decisao,
               pct_v_ko_tko, pct_v_sub, pct_v_decisao,
               pct_d_ko_tko, pct_d_sub, pct_d_decisao
        FROM gold.v_probabilidade_metodo
        WHERE id_lutador = :fid
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def _buscar_historico(engine: Any, fighter_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT resultado, oponente, data_evento,
               knockdowns, strikes_total, takedowns, tentativas_sub,
               method_short, method_detail, round_fim, fight_time, title_bout
        FROM gold.v_historico_lutador
        WHERE id_lutador = :fid
        ORDER BY TRY_CAST(data_evento AS DATE) DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fid": fighter_id})


def carregar_dados(engine: Any, nome: str) -> DadosLutador:
    """Carrega todos os dados de um lutador a partir do banco."""
    df_stats = _buscar_lutador(engine, nome)
    if df_stats.empty:
        print(f"[ERRO] Lutador não encontrado: '{nome}'")
        sys.exit(1)

    if len(df_stats) > 1:
        print(f"[AVISO] Múltiplos resultados para '{nome}'. Usando o primeiro:")
        for _, r in df_stats.iterrows():
            print(f"  - {r['nome_lutador']} ({r['vitorias']}W-{r['derrotas']}L)")
        print()

    row = df_stats.iloc[0]
    fid = int(row["id_lutador"])
    d = DadosLutador(nome=row["nome_lutador"])
    d.stats = row.to_dict()
    d.curva_fadiga = _buscar_curva_fadiga(engine, fid)
    d.troca_dano = _buscar_troca_dano(engine, fid)
    d.timing_finalizacao = _buscar_timing_finalizacao(engine, fid)
    d.mapa_alvos = _buscar_mapa_alvos(engine, fid)

    df_metodo = _buscar_prob_metodo(engine, fid)
    d.prob_metodo = df_metodo.iloc[0].to_dict() if not df_metodo.empty else {}
    d.historico = _buscar_historico(engine, fid)
    return d
