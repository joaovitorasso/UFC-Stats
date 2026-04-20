import logging
import logging.config
from pathlib import Path

import yaml


def setup_logging(repo_root: Path) -> None:
    cfg_path = repo_root / "configs" / "logging.yaml"
    if cfg_path.exists():
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def attach_db_handler(banco, *, run_id: str, origem: str) -> None:
    """
    Anexa o DbLogHandler ao root logger.
    Silenciosamente ignora se a conexão não estiver disponível.
    banco: instância de ConexaoBanco
    """
    try:
        from ufc_pipeline.banco import montar_conn_str
        from ufc_pipeline.db_log_handler import DbLogHandler

        conn_str = montar_conn_str(banco)
        if not conn_str:
            return

        handler = DbLogHandler(conn_str, run_id=run_id, origem=origem)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
    except Exception:
        pass  # logging para BD é melhor-esforço; nunca falhar o processo
