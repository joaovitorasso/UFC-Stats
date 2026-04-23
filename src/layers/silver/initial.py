import pandas as pd

from layers.silver.db import _executar_sql_arquivo, _salvar_tabela


def _salvar_inicial(
    engine,
    schema: str,
    silver_eventos_out: pd.DataFrame,
    silver_lutas_out: pd.DataFrame,
    silver_lutadores_out: pd.DataFrame,
    silver_historico_lutas: pd.DataFrame,
) -> None:
    """Carga inicial: dropa e recria todas as tabelas fato."""
    _executar_sql_arquivo(engine, "silver_eventos.sql", schema)
    _salvar_tabela(silver_eventos_out, engine, schema, "eventos", if_exists="append")
    _executar_sql_arquivo(engine, "silver_lutas.sql", schema)
    _salvar_tabela(silver_lutas_out, engine, schema, "lutas", if_exists="append")
    _executar_sql_arquivo(engine, "silver_lutadores.sql", schema)
    _salvar_tabela(silver_lutadores_out, engine, schema, "lutadores", if_exists="append")
    _executar_sql_arquivo(engine, "silver_historico_lutas.sql", schema)
    _salvar_tabela(silver_historico_lutas, engine, schema, "historico_lutas", if_exists="append")
