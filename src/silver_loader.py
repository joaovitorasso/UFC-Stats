import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from orchestration.orchestrator import Orchestrator

log = logging.getLogger(__name__)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)

    parser = argparse.ArgumentParser(description="Carga de bronze (arquivos) para silver (SQL Server).")
    parser.add_argument("--dt", default=None, help="Particao dt=YYYY-MM-DD. Se vazio, usa a execucao mais recente do pipeline.")
    parser.add_argument("--data-root", default="./data", help="Pasta base de dados locais.")
    args = parser.parse_args()

    orch = Orchestrator(repo_root, dt=args.dt)
    data_root = Path(args.data_root).resolve()

    total_eventos, total_lutas, total_lutadores, total_historico_lutas = orch.executar_silver(data_root)

    log.info("Carga silver finalizada com sucesso.")
    print(
        f"Silver carregada: eventos={total_eventos}, "
        f"lutas={total_lutas}, lutadores={total_lutadores}, historico_lutas={total_historico_lutas}"
    )


if __name__ == "__main__":
    main()
