import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

from orchestration.orchestrator import Orchestrator
from layers.silver.db import registrar_run_pipeline
from utils.banco import ConexaoBanco


def main():
    parser = argparse.ArgumentParser(prog="pipeline.py")
    modo = parser.add_mutually_exclusive_group(required=True)
    modo.add_argument("--initial", action="store_true")
    modo.add_argument("--incremental", action="store_true")
    parser.add_argument("--dt", default=None)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)

    orch = Orchestrator(repo_root, dt=args.dt)
    artefatos = orch.executar_bronze(full_load=args.initial)

    try:
        banco = ConexaoBanco.do_env()
        registrar_run_pipeline(
            banco,
            run_id=artefatos["run_id"],
            dt=artefatos["dt"],
            tipo_carga="initial" if args.initial else "incremental",
            estagio="bronze",
            iniciado_em=orch.iniciado_em,
        )
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("Nao foi possivel registrar o run: %s", exc)

    print(json.dumps(artefatos, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
