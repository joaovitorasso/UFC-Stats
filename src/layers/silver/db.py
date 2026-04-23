import logging
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import Integer, String, create_engine, text

from utils.banco import ConexaoBanco, montar_conn_str

_SQL_DIR = Path(__file__).resolve().parents[3] / "sql"

log = logging.getLogger(__name__)


def _executar_sql_arquivo(engine, nome_arquivo: str, schema: str) -> None:
    """Carrega um arquivo .sql de _SQL_DIR, substitui o schema e executa cada statement."""
    sql_text = (_SQL_DIR / nome_arquivo).read_text(encoding="utf-8")
    sql_text = sql_text.replace("[silver]", f"[{schema}]")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


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


def _salvar_tabela(df: pd.DataFrame, engine, schema: str, tabela: str, if_exists: str = "replace") -> None:
    from layers.silver.transformers import _formatar_data_ddmmaaaa
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
        if_exists=if_exists,
        index=False,
        dtype=dtype_override,
    )


def _garantir_pipeline_runs(engine, schema: str) -> None:
    _executar_sql_arquivo(engine, "pipeline_runs.sql", schema)


def _consultar_tipo_carga(engine, schema: str, dt: str | None) -> str:
    if dt:
        query = text(f"""
            SELECT TOP 1 tipo_carga
            FROM [{schema}].[pipeline_runs]
            WHERE dt = :dt AND status = 'concluido'
            ORDER BY iniciado_em DESC
        """)
        params: dict = {"dt": dt}
    else:
        query = text(f"""
            SELECT TOP 1 tipo_carga
            FROM [{schema}].[pipeline_runs]
            WHERE status = 'concluido'
            ORDER BY iniciado_em DESC
        """)
        params = {}

    with engine.connect() as conn:
        row = conn.execute(query, params).fetchone()

    if not row:
        raise RuntimeError(
            f"Nenhuma execucao de pipeline encontrada para dt={dt!r}. Execute o pipeline antes do silver_loader."
        )
    return row[0]


def registrar_run_pipeline(
    banco: "ConexaoBanco",
    run_id: str,
    dt: str,
    tipo_carga: str,
    estagio: str,
    iniciado_em: "datetime",
) -> None:
    engine = _criar_engine(banco)
    try:
        _garantir_schema(engine, banco.silver_schema)
        _garantir_pipeline_runs(engine, banco.silver_schema)
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO [{banco.silver_schema}].[pipeline_runs]
                        (run_id, dt, tipo_carga, estagio, status, iniciado_em, concluido_em)
                    VALUES
                        (:run_id, :dt, :tipo_carga, :estagio, 'concluido', :iniciado_em, SYSUTCDATETIME())
                """),
                {
                    "run_id": run_id,
                    "dt": dt,
                    "tipo_carga": tipo_carga,
                    "estagio": estagio,
                    "iniciado_em": iniciado_em.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
        log.info("[pipeline_runs] run_id=%s registrado: tipo=%s dt=%s estagio=%s", run_id, tipo_carga, dt, estagio)
    finally:
        engine.dispose()
