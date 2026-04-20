"""
Predição completa de um evento UFC.

Uso:
    python scripts/prever_evento.py --evento "UFC 315"
    python scripts/prever_evento.py --event-id <id_evento>
    python scripts/prever_evento.py --listar
    python scripts/prever_evento.py --listar --evento "UFC"

Consulta as lutas do evento em silver.lutas e os lutadores em
silver.lutas_lutadores, executa a predição para cada luta e salva
os resultados em gold.predicoes_evento.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ufc_pipeline.banco import ConexaoBanco  # type: ignore
from ufc_pipeline.predicao import criar_engine  # type: ignore
from ufc_pipeline.predicao.executor import (  # type: ignore
    executar_predicao_luta,
    salvar_predicoes,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Consultas ─────────────────────────────────────────────────────────────────

def buscar_lutas_evento(engine, *, evento: str | None, event_id: str | None) -> pd.DataFrame:
    if event_id:
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
            return pd.read_sql(sql, conn, params={"eid": event_id})
    else:
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
            WHERE l.nome_evento LIKE :nome
            ORDER BY l.id_evento, TRY_CAST(l.ordem_luta AS INT)
        """)
        with engine.connect() as conn:
            return pd.read_sql(sql, conn, params={"nome": f"%{evento}%"})


def listar_eventos_disponiveis(engine, termo: str) -> pd.DataFrame:
    sql = text("""
        SELECT DISTINCT nome_evento, id_evento
        FROM silver.lutas
        WHERE nome_evento LIKE :nome
        ORDER BY nome_evento
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"nome": f"%{termo}%"})


# ── Exibição ──────────────────────────────────────────────────────────────────

SEP       = "=" * 80
SEP_MINOR = "-" * 80


def imprimir_resumo_evento(rows: list[dict]) -> None:
    if not rows:
        print("  Nenhuma predição disponível.")
        return

    nome_evento = rows[0].get("nome_evento", "?")
    data_evento = rows[0].get("data_evento", "?")
    print(f"\n{SEP}")
    print(f"  PREDIÇÕES — {nome_evento.upper()}")
    print(f"  Data: {data_evento}")
    print(SEP)
    print(f"  {'#':<4}  {'LUTADOR A':<25}  {'PCT':>6}  {'LUTADOR B':<25}  {'PCT':>6}  "
          f"{'VENCEDOR':<25}  {'MÉTODO':<15}  {'CONF'}")
    print(SEP_MINOR)
    for r in sorted(rows, key=lambda x: int(x.get("ordem_luta") or 99)):
        ordem  = str(r.get("ordem_luta") or "?")
        na     = (r.get("nome_lutador_a") or "?")[:24]
        nb     = (r.get("nome_lutador_b") or "?")[:24]
        pa     = r.get("probabilidade_a", 0)
        pb     = r.get("probabilidade_b", 0)
        venc   = (r.get("vencedor_previsto") or "?")[:24]
        metodo = (r.get("metodo_previsto") or "?")[:14]
        conf   = r.get("confianca", "?")
        print(f"  {ordem:<4}  {na:<25}  {pa:>5.1f}%  {nb:<25}  {pb:>5.1f}%  "
              f"{venc:<25}  {metodo:<15}  {conf}")
    print(SEP)
    print(f"  Total de lutas previstas: {len(rows)}")
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Predição de evento UFC completo — salva em gold.predicoes_evento"
    )
    parser.add_argument("--evento",   default=None, help='Nome do evento, ex: "UFC 315"')
    parser.add_argument("--event-id", default=None, help="ID inteiro do evento (id_evento)")
    parser.add_argument("--listar",   action="store_true", help="Lista eventos disponíveis e sai")
    args = parser.parse_args()

    if not args.evento and not args.event_id and not args.listar:
        parser.error("Informe --evento ou --event-id (ou --listar para ver eventos disponíveis).")

    banco  = ConexaoBanco.do_env()
    engine = criar_engine(banco)
    run_id = uuid.uuid4().hex[:12]

    if args.listar:
        df_ev = listar_eventos_disponiveis(engine, args.evento or "")
        if df_ev.empty:
            print("Nenhum evento encontrado.")
        else:
            print(f"\n{'EVENTO':<50}  ID_EVENTO")
            print("-" * 70)
            for _, row in df_ev.iterrows():
                print(f"  {str(row['nome_evento']):<50}  {row['id_evento']}")
        return

    log.info("Buscando lutas para evento: '%s'...", args.evento or args.event_id)
    df_lutas = buscar_lutas_evento(engine, evento=args.evento, event_id=args.event_id)

    if df_lutas.empty:
        print(f"[ERRO] Nenhuma luta encontrada para '{args.evento or args.event_id}'.")
        print("Use --listar para ver os eventos disponíveis.")
        sys.exit(1)

    eventos_encontrados = df_lutas["nome_evento"].unique().tolist()
    if len(eventos_encontrados) > 1:
        print("[AVISO] Múltiplos eventos encontrados:")
        for ev in eventos_encontrados:
            print(f"  - {ev}")
        print("Use um nome mais específico ou --event-id para escolher um.")
        sys.exit(1)

    nome_evento  = eventos_encontrados[0]
    total_lutas  = len(df_lutas)
    log.info("Evento: %s | %d lutas encontradas.", nome_evento, total_lutas)

    resultados: list[dict] = []
    erros = 0
    for i, (_, fight_row) in enumerate(df_lutas.iterrows(), start=1):
        nome_a = str(fight_row.get("nome_lutador_a") or "?")
        nome_b = str(fight_row.get("nome_lutador_b") or "?")
        log.info("[%d/%d] Prevendo: %s vs %s", i, total_lutas, nome_a, nome_b)
        resultado = executar_predicao_luta(engine, fight_row, run_id)
        if resultado:
            resultados.append(resultado)
        else:
            erros += 1

    log.info("Predições: %d OK, %d sem dados suficientes.", len(resultados), erros)

    if resultados:
        log.info("Salvando %d predições em gold.predicoes_evento...", len(resultados))
        n = salvar_predicoes(engine, resultados)
        log.info("Salvo: %d linhas.", n)
    else:
        log.warning("Nenhuma predição para salvar.")

    imprimir_resumo_evento(resultados)

    if erros:
        print(f"\n  [AVISO] {erros} luta(s) sem dados suficientes foram ignoradas.")


if __name__ == "__main__":
    main()
