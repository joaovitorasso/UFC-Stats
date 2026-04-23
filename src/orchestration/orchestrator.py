import uuid
from datetime import datetime, timezone
from pathlib import Path

from utils.config import load_config
from utils.logger import setup_logging


class Orchestrator:
    def __init__(self, repo_root: Path, dt: str | None = None):
        self.repo_root = repo_root
        setup_logging(repo_root)
        self.cfg, self.sources = load_config(repo_root)
        self.dt = dt or datetime.utcnow().strftime("%Y-%m-%d")
        self.run_id = uuid.uuid4().hex[:12]
        self.iniciado_em = datetime.now(timezone.utc)

    def executar_bronze(self, full_load: bool) -> dict:
        from orchestration.initial_load import InitialLoadStrategy
        from orchestration.incremental_load import IncrementalLoadStrategy
        strategy = InitialLoadStrategy(self) if full_load else IncrementalLoadStrategy(self)
        return strategy.executar()

    def executar_silver(self, data_root: Path) -> tuple[int, int, int, int]:
        import pandas as pd
        from utils.helpers import read_jsonl
        from utils.banco import ConexaoBanco
        from layers.silver import db as silver_db
        from layers.silver import transformers, dimensions, initial, incremental

        bronze_root = data_root / "bronze"

        def _ler_jsonl_particionado(base_dir: Path, nome_arquivo: str, dt: str | None) -> pd.DataFrame:
            frames: list[pd.DataFrame] = []
            if dt:
                arquivo = base_dir / f"dt={dt}" / nome_arquivo
                if not arquivo.exists():
                    raise FileNotFoundError(f"Arquivo nao encontrado: {arquivo}")
                df = pd.read_json(arquivo, lines=True)
                df["dt_particao"] = dt
                frames.append(df)
            else:
                for particao in sorted(base_dir.glob("dt=*")):
                    if not particao.is_dir():
                        continue
                    dt_valor = particao.name.replace("dt=", "")
                    arquivo = particao / nome_arquivo
                    if arquivo.exists():
                        df = pd.read_json(arquivo, lines=True)
                        df["dt_particao"] = dt_valor
                        frames.append(df)
                if not frames:
                    raise FileNotFoundError(f"Nenhum arquivo encontrado em: {base_dir}")
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        bronze_eventos = _ler_jsonl_particionado(bronze_root / "eventos", "eventos.jsonl", self.dt)
        bronze_lutas = _ler_jsonl_particionado(bronze_root / "lutas", "lutas.jsonl", self.dt)
        bronze_lutadores = _ler_jsonl_particionado(bronze_root / "lutadores", "lutadores.jsonl", self.dt)

        silver_eventos = transformers._preparar_silver_eventos(bronze_eventos)
        silver_lutas = transformers._preparar_silver_lutas(bronze_lutas)
        silver_lutadores = transformers._preparar_silver_lutadores(bronze_lutadores)

        banco = ConexaoBanco.do_env()
        engine = silver_db._criar_engine(banco)

        try:
            silver_db._garantir_schema(engine, banco.silver_schema)
            silver_db._garantir_pipeline_runs(engine, banco.silver_schema)
            tipo_carga = silver_db._consultar_tipo_carga(engine, banco.silver_schema, self.dt)
            full_load = tipo_carga == "initial"

            with engine.begin() as conn:
                from sqlalchemy import text
                conn.execute(text(f"IF OBJECT_ID('[{banco.silver_schema}].[lutadores_lutas]', 'U') IS NOT NULL DROP TABLE [{banco.silver_schema}].[lutadores_lutas]"))
                conn.execute(text(f"IF OBJECT_ID('[{banco.silver_schema}].[historico_lutador]', 'U') IS NOT NULL DROP TABLE [{banco.silver_schema}].[historico_lutador]"))

            dimensions._garantir_dim_bonus(engine, banco.silver_schema)
            dimensions._garantir_dim_evento(engine, banco.silver_schema)
            dimensions._garantir_dim_luta(engine, banco.silver_schema)
            dimensions._garantir_dim_lutador(engine, banco.silver_schema)

            dimensions._upsert_dim_bonus(engine, banco.silver_schema, silver_lutas)
            mapa_dim_evento = dimensions._upsert_dim_evento(engine, banco.silver_schema, silver_eventos)
            mapa_dim_luta = dimensions._upsert_dim_luta(engine, banco.silver_schema, silver_lutas, mapa_dim_evento)
            mapa_dim_lutador = dimensions._upsert_dim_lutador(engine, banco.silver_schema, silver_lutadores, silver_lutas)

            silver_eventos_ids = transformers._aplicar_id_evento(silver_eventos, mapa_dim_evento)
            silver_lutas_ids = transformers._aplicar_ids_luta(silver_lutas, mapa_dim_evento, mapa_dim_luta)
            silver_lutadores_ids = transformers._aplicar_id_lutador(silver_lutadores, mapa_dim_lutador)
            silver_historico_lutas = transformers._preparar_silver_historico_lutador(
                silver_lutas_ids,
                mapa_dim_lutador,
                mapa_dim_luta,
            )

            silver_eventos_out = silver_eventos_ids.drop(columns=["event_id"], errors="ignore")
            silver_eventos_out = silver_eventos_out.drop(columns=["event_url"], errors="ignore")
            silver_lutas_out = silver_lutas_ids.drop(
                columns=["fight_id", "event_id", "fight_url", "lutadores_json"],
                errors="ignore",
            )
            silver_lutadores_out = silver_lutadores_ids.drop(columns=["fighter_id", "url_perfil"], errors="ignore")

            if full_load:
                initial._salvar_inicial(engine, banco.silver_schema, silver_eventos_out, silver_lutas_out, silver_lutadores_out, silver_historico_lutas)
            else:
                incremental._salvar_incremental(engine, banco.silver_schema, silver_eventos_out, silver_lutas_out, silver_lutadores_out, silver_historico_lutas)
        finally:
            engine.dispose()

        return len(silver_eventos), len(silver_lutas), len(silver_lutadores), len(silver_historico_lutas)
