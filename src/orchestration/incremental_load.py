from layers.bronze import events, fights, fighters


class IncrementalLoadStrategy:
    def __init__(self, orchestrator):
        self.orch = orchestrator

    def executar(self) -> dict:
        orch = self.orch
        index_path = events.coletar_index(orch.cfg, orch.sources, dt=orch.dt, run_id=orch.run_id, full_load=False)
        html_eventos_dir = events.baixar_html(orch.cfg, index_path, dt=orch.dt, run_id=orch.run_id, use_cache=False)
        events.gerar_bronze(orch.cfg, index_path, dt=orch.dt, run_id=orch.run_id)
        lutas_path, fighters_index_path = fights.gerar_bronze(
            orch.cfg, index_path, html_eventos_dir, dt=orch.dt, run_id=orch.run_id, use_cache=False
        )
        html_lutadores_dir = fighters.baixar_html(orch.cfg, fighters_index_path, dt=orch.dt, run_id=orch.run_id, use_cache=False)
        fighters.gerar_bronze(orch.cfg, fighters_index_path, html_lutadores_dir, dt=orch.dt, run_id=orch.run_id)
        return {
            "run_id": orch.run_id,
            "dt": orch.dt,
            "full_load": False,
            "bronze_lutas": str(lutas_path),
        }
