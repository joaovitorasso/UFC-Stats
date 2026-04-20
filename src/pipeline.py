import argparse
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

import coleta
from config import load_config
from logger import setup_logging

log = logging.getLogger(__name__)


def _data_hoje() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def executar_estagio(estagio: str, repo_root: Path, *, dt: str | None = None) -> dict:
    setup_logging(repo_root)
    cfg, sources = load_config(repo_root)
    run_id = uuid.uuid4().hex[:12]
    dt = dt or _data_hoje()

    artefatos: dict = {"run_id": run_id, "dt": dt}
    log.info("=== PIPELINE INICIADA | run_id=%s | dt=%s | estagio=%s ===", run_id, dt, estagio)

    # RAW
    log.info("[RAW] Coletando índice de eventos...")
    caminho_index = coleta.coletar_eventos_index(cfg, sources, dt=dt, run_id=run_id)
    artefatos["raw_eventos_index"] = str(caminho_index)
    log.info("[RAW] Índice salvo em: %s", caminho_index)

    log.info("[RAW] Baixando HTML dos eventos...")
    dir_html_eventos = coleta.baixar_html_eventos(cfg, caminho_index, dt=dt, run_id=run_id)
    artefatos["raw_html_eventos"] = str(dir_html_eventos)
    log.info("[RAW] HTMLs de eventos salvos em: %s", dir_html_eventos)

    if estagio == "raw":
        log.info("=== PIPELINE CONCLUÍDA (raw) | run_id=%s ===", run_id)
        return artefatos

    # BRONZE
    log.info("[BRONZE] Gerando bronze de eventos...")
    caminho_eventos = coleta.gerar_bronze_eventos(cfg, caminho_index, dt=dt, run_id=run_id)
    artefatos["bronze_eventos"] = str(caminho_eventos)
    log.info("[BRONZE] Eventos bronze: %s", caminho_eventos)

    log.info("[BRONZE] Gerando bronze de lutas...")
    caminho_lutas, caminho_index_lutadores = coleta.gerar_bronze_lutas(
        cfg, caminho_index, dir_html_eventos, dt=dt, run_id=run_id
    )
    artefatos["bronze_lutas"] = str(caminho_lutas)
    artefatos["bronze_index_lutadores"] = str(caminho_index_lutadores)
    log.info("[BRONZE] Lutas bronze: %s", caminho_lutas)

    log.info("[RAW] Baixando HTML dos lutadores...")
    dir_html_lutadores = coleta.baixar_html_lutadores(cfg, caminho_index_lutadores, dt=dt, run_id=run_id)
    artefatos["raw_html_lutadores"] = str(dir_html_lutadores)
    log.info("[RAW] HTMLs de lutadores salvos em: %s", dir_html_lutadores)

    log.info("[BRONZE] Gerando bronze de lutadores...")
    caminho_lutadores = coleta.gerar_bronze_lutadores(
        cfg, caminho_index_lutadores, dir_html_lutadores, dt=dt, run_id=run_id
    )
    artefatos["bronze_lutadores"] = str(caminho_lutadores)
    log.info("[BRONZE] Lutadores bronze: %s", caminho_lutadores)

    log.info("=== PIPELINE CONCLUÍDA | run_id=%s ===", run_id)
    return artefatos


def main() -> None:
    parser = argparse.ArgumentParser(prog="pipeline.py", description="UFC Lakehouse Pipeline (RAW/Bronze)")
    parser.add_argument("comando", choices=["run"], help="Comando")
    parser.add_argument("estagio", choices=["raw", "bronze", "all"], help="Estagio")
    parser.add_argument("--dt", default=None, help="Particao dt=YYYY-MM-DD (default: hoje UTC)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    estagio = "bronze" if args.estagio == "all" else args.estagio

    artefatos = executar_estagio(estagio, repo_root, dt=args.dt)
    print(json.dumps(artefatos, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
