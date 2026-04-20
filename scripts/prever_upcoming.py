"""
Predição automática de todos os eventos UFC com status 'upcoming'.

Uso:
    python scripts/prever_upcoming.py
    python scripts/prever_upcoming.py --dry-run          # lista eventos, não prevê
    python scripts/prever_upcoming.py --force            # reprevê mesmo que já existam predições
    python scripts/prever_upcoming.py --evento "UFC 315" # filtra por nome

O script:
  1. Busca todos os eventos com status upcoming em silver.eventos
  2. Para cada evento, carrega as lutas de silver.lutas e os lutadores de silver.lutas_lutadores
  3. Executa todos os módulos de análise para cada luta
  4. Salva / atualiza os resultados em gold.predicoes_evento (MERGE por id_luta)
  5. Imprime um resumo consolidado ao final
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ufc_pipeline.banco import ConexaoBanco  # type: ignore
from ufc_pipeline.predicao import criar_engine  # type: ignore
from ufc_pipeline.predicao.executor import (  # type: ignore
    executar_predicao_luta,
    salvar_predicoes,
    TABLE_PREDICOES,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SEP       = "=" * 80
SEP_MINOR = "-" * 80


# ── Consultas ─────────────────────────────────────────────────────────────────

def buscar_eventos_upcoming(engine: Any, filtro_nome: str | None = None) -> pd.DataFrame:
    """Retorna todos os eventos com status 'upcoming', opcionalmente filtrados por nome."""
    where_extra = "AND e.nome LIKE :nome" if filtro_nome else ""
    sql = text(f"""
        SELECT e.id_evento, e.nome, e.data_evento, e.local, e.status
        FROM silver.eventos e
        WHERE e.status LIKE '%upcoming%'
          {where_extra}
        ORDER BY TRY_CAST(e.data_evento AS DATE)
    """)
    params = {"nome": f"%{filtro_nome}%"} if filtro_nome else {}
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def buscar_lutas_evento(engine: Any, id_evento: int) -> pd.DataFrame:
    sql = text("""
        WITH lutadores_base AS (
            SELECT DISTINCT
                   ll.id_luta,
                   ll.id_lutador,
                   ll.nome_lutador,
                   ll.fighter_id_hash
            FROM silver.lutas_lutadores ll
        ),
        lutadores_rank AS (
            SELECT
                lb.id_luta,
                lb.id_lutador,
                lb.nome_lutador,
                lb.fighter_id_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY lb.id_luta
                    ORDER BY
                        CASE WHEN lb.id_lutador IS NULL THEN 1 ELSE 0 END,
                        lb.id_lutador,
                        lb.fighter_id_hash
                ) AS rn
            FROM lutadores_base lb
        ),
        luta_pairs AS (
            SELECT
                id_luta,
                MAX(CASE WHEN rn = 1 THEN id_lutador END)    AS id_lutador_a,
                MAX(CASE WHEN rn = 1 THEN nome_lutador END)  AS nome_lutador_a,
                MAX(CASE WHEN rn = 2 THEN id_lutador END)    AS id_lutador_b,
                MAX(CASE WHEN rn = 2 THEN nome_lutador END)  AS nome_lutador_b
            FROM lutadores_rank
            WHERE rn <= 2
            GROUP BY id_luta
        )
        SELECT l.id_luta, l.id_evento,
               l.nome_evento, e.data_evento,
               l.ordem_luta, l.tipo_luta,
               lp.id_lutador_a, lp.nome_lutador_a,
               lp.id_lutador_b, lp.nome_lutador_b
        FROM silver.lutas l
        LEFT JOIN silver.eventos e ON l.id_evento = e.id_evento
        LEFT JOIN luta_pairs lp ON lp.id_luta = l.id_luta
        WHERE l.id_evento = :eid
        ORDER BY TRY_CAST(l.ordem_luta AS INT)
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"eid": id_evento})


def predicoes_ja_existem(engine: Any, id_evento: int) -> bool:
    """Verifica se já existem predições salvas para este evento."""
    sql = text("""
        SELECT 1 FROM gold.predicoes_evento WHERE id_evento = :eid
    """)
    try:
        with engine.connect() as conn:
            return conn.execute(sql, {"eid": id_evento}).first() is not None
    except Exception:
        return False


# ── Exibição ──────────────────────────────────────────────────────────────────

def _imprimir_cabecalho_evento(nome: str, data: str, local: str, n_lutas: int) -> None:
    print(f"\n{SEP}")
    print(f"  {nome.upper()}")
    print(f"  Data: {data}  |  Local: {local}  |  {n_lutas} luta(s)")
    print(SEP)


def _imprimir_tabela_predicoes(rows: list[dict], nome_a_width: int = 25) -> None:
    if not rows:
        print("  Nenhuma predição disponível.")
        return
    print(f"  {'#':<4}  {'LUTADOR A':<{nome_a_width}}  {'PCT':>6}  "
          f"{'LUTADOR B':<{nome_a_width}}  {'PCT':>6}  "
          f"{'VENCEDOR':<{nome_a_width}}  {'MÉTODO':<15}  CONF")
    print(SEP_MINOR)
    for r in sorted(rows, key=lambda x: int(x.get("ordem_luta") or 99)):
        ordem  = str(r.get("ordem_luta") or "?")
        na     = (r.get("nome_lutador_a") or "?")[:nome_a_width - 1]
        nb     = (r.get("nome_lutador_b") or "?")[:nome_a_width - 1]
        pa     = r.get("probabilidade_a", 0)
        pb     = r.get("probabilidade_b", 0)
        venc   = (r.get("vencedor_previsto") or "?")[:nome_a_width - 1]
        metodo = (r.get("metodo_previsto") or "?")[:14]
        conf   = r.get("confianca", "?")
        print(f"  {ordem:<4}  {na:<{nome_a_width}}  {pa:>5.1f}%  "
              f"{nb:<{nome_a_width}}  {pb:>5.1f}%  "
              f"{venc:<{nome_a_width}}  {metodo:<15}  {conf}")


def _imprimir_resumo_final(resultados_por_evento: dict[str, list[dict]], total_erros: int) -> None:
    print(f"\n{SEP}")
    print("  RESUMO FINAL — EVENTOS UPCOMING")
    print(SEP)
    total_predicoes = 0
    for nome_evento, rows in resultados_por_evento.items():
        total_predicoes += len(rows)
        print(f"  {nome_evento:<50}  {len(rows)} predição(ões)")
    print(SEP_MINOR)
    print(f"  Total de eventos processados: {len(resultados_por_evento)}")
    print(f"  Total de predições salvas:    {total_predicoes}")
    if total_erros:
        print(f"  Lutas sem dados suficientes:  {total_erros}")
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Predição automática de todos os eventos UFC upcoming"
    )
    parser.add_argument(
        "--evento", default=None,
        help="Filtra por nome de evento (ex: 'UFC 315'). Sem filtro = todos os upcoming."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Lista os eventos e lutas encontrados sem executar predições."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Reprevê eventos mesmo que já existam predições salvas no banco."
    )
    args = parser.parse_args()

    banco  = ConexaoBanco.do_env()
    engine = criar_engine(banco)
    run_id = uuid.uuid4().hex[:12]

    # ── Buscar eventos upcoming ───────────────────────────────────────────────
    log.info("Buscando eventos upcoming...")
    df_eventos = buscar_eventos_upcoming(engine, filtro_nome=args.evento)

    if df_eventos.empty:
        filtro_str = f" com filtro '{args.evento}'" if args.evento else ""
        print(f"[INFO] Nenhum evento upcoming encontrado{filtro_str}.")
        print("  Verifique se a coluna 'status' em silver.eventos contém 'upcoming'.")
        sys.exit(0)

    print(f"\n{SEP}")
    print(f"  EVENTOS UPCOMING ENCONTRADOS ({len(df_eventos)})")
    print(SEP)
    for _, ev in df_eventos.iterrows():
        print(f"  [{ev['id_evento']}]  {ev['nome']:<50}  {ev['data_evento'] or '—'}")
    print(SEP)

    if args.dry_run:
        print("\n[dry-run] Listagem concluída. Nenhuma predição executada.")
        sys.exit(0)

    # ── Processar cada evento ─────────────────────────────────────────────────
    resultados_por_evento: dict[str, list[dict]] = {}
    total_erros = 0

    for _, ev in df_eventos.iterrows():
        id_evento   = int(ev["id_evento"])
        nome_evento = str(ev["nome"])
        data_evento = str(ev["data_evento"] or "")
        local       = str(ev.get("local") or "")

        # Verifica se já existem predições (a menos que --force)
        if not args.force and predicoes_ja_existem(engine, id_evento):
            log.info("[%s] Predições já existem. Use --force para refazer. Pulando.", nome_evento)
            continue

        df_lutas = buscar_lutas_evento(engine, id_evento)
        if df_lutas.empty:
            log.warning("[%s] Nenhuma luta encontrada em silver.lutas. Pulando.", nome_evento)
            continue

        _imprimir_cabecalho_evento(nome_evento, data_evento, local, len(df_lutas))

        resultados: list[dict] = []
        erros_evento = 0

        for i, (_, fight_row) in enumerate(df_lutas.iterrows(), start=1):
            nome_a = str(fight_row.get("nome_lutador_a") or "?")
            nome_b = str(fight_row.get("nome_lutador_b") or "?")
            log.info("  [%d/%d] %s vs %s", i, len(df_lutas), nome_a, nome_b)

            resultado = executar_predicao_luta(engine, fight_row, run_id)
            if resultado:
                resultados.append(resultado)
            else:
                erros_evento += 1

        if resultados:
            n = salvar_predicoes(engine, resultados)
            log.info("[%s] %d predições salvas em %s.", nome_evento, n, TABLE_PREDICOES)
            _imprimir_tabela_predicoes(resultados)
            resultados_por_evento[nome_evento] = resultados
        else:
            log.warning("[%s] Nenhuma predição gerada.", nome_evento)

        if erros_evento:
            log.warning("[%s] %d luta(s) sem dados suficientes.", nome_evento, erros_evento)
        total_erros += erros_evento

    # ── Resumo final ──────────────────────────────────────────────────────────
    if resultados_por_evento:
        _imprimir_resumo_final(resultados_por_evento, total_erros)
    else:
        print("\n[INFO] Nenhuma predição nova gerada.")
        if not args.force:
            print("  Use --force para refazer predições já existentes.")


if __name__ == "__main__":
    main()
