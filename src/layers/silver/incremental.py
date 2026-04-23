import logging

import pandas as pd
from sqlalchemy import text

from layers.silver.db import _salvar_tabela

log = logging.getLogger(__name__)


def _salvar_incremental(
    engine,
    schema: str,
    silver_eventos_out: pd.DataFrame,
    silver_lutas_out: pd.DataFrame,
    silver_lutadores_out: pd.DataFrame,
    silver_historico_lutas: pd.DataFrame,
) -> None:
    """Carga incremental: deleta registros do escopo atual + stale, reinsere o que veio do bronze."""

    def _ids(df: pd.DataFrame, col: str) -> set[int]:
        return {int(x) for x in df[col].dropna() if not pd.isna(x)}

    id_eventos_atuais = _ids(silver_eventos_out, "id_evento")
    id_lutadores_atuais = _ids(silver_lutadores_out, "id_lutador")

    # Eventos que estavam em scope no banco (upcoming ou nos últimos 30 dias) mas sumiram do bronze
    with engine.connect() as conn:
        scope_df = pd.read_sql_query(
            text(f"""
                SELECT id_evento
                FROM [{schema}].[eventos]
                WHERE status = 'upcoming'
                   OR TRY_CONVERT(date, data_evento, 103) >= CAST(DATEADD(day, -30, GETUTCDATE()) AS date)
            """),
            conn,
        )
    id_eventos_em_scope: set[int] = _ids(scope_df, "id_evento") if not scope_df.empty else set()
    id_eventos_stale = id_eventos_em_scope - id_eventos_atuais

    if id_eventos_stale:
        log.info("[silver] incremental: %d evento(s) stale a remover: %s", len(id_eventos_stale), id_eventos_stale)

    # Deletar: scope atual (será reinserido atualizado) + stale (desapareceu do site)
    id_eventos_deletar = id_eventos_atuais | id_eventos_stale
    if id_eventos_deletar:
        ids_str = ",".join(str(i) for i in id_eventos_deletar)
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM [{schema}].[historico_lutas] WHERE id_evento IN ({ids_str})"))
            conn.execute(text(f"DELETE FROM [{schema}].[lutas] WHERE id_evento IN ({ids_str})"))
            conn.execute(text(f"DELETE FROM [{schema}].[eventos] WHERE id_evento IN ({ids_str})"))

    # Deletar lutadores do scope atual para atualizar cartel e stats
    if id_lutadores_atuais:
        ids_str = ",".join(str(i) for i in id_lutadores_atuais)
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM [{schema}].[lutadores] WHERE id_lutador IN ({ids_str})"))

    # Inserir dados atualizados
    _salvar_tabela(silver_eventos_out, engine, schema, "eventos", if_exists="append")
    _salvar_tabela(silver_lutas_out, engine, schema, "lutas", if_exists="append")
    _salvar_tabela(silver_lutadores_out, engine, schema, "lutadores", if_exists="append")
    _salvar_tabela(silver_historico_lutas, engine, schema, "historico_lutas", if_exists="append")
