"""
Módulo: Índice de Cardio Composto
Peso padrão: 3/5

Score composto que combina índice de fadiga, volume nos rounds tardios
e experiência acumulada em rounds. Complementa curva_fadiga com
uma visão mais abrangente do condicionamento.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def score(d: DadosLutador) -> float:
        cardio       = _v(d.stats, "indice_cardio", 80.0)
        media_r3     = _v(d.stats, "media_golpes_r3plus")
        media_r1     = _v(d.stats, "media_golpes_r1", 1.0)
        total_rounds = _v(d.stats, "total_rounds")
        vol_score    = (media_r3 / max(media_r1, 1)) * 50
        rounds_score = min(total_rounds / 10, 50.0)
        return round(cardio * 0.4 + vol_score * 0.4 + rounds_score * 0.2, 1)

    sa = score(a)
    sb = score(b)
    return {
        "score_a": sa,
        "score_b": sb,
        "vantagem": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
