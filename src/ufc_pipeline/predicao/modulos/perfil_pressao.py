"""
Módulo: Perfil de Pressão
Peso padrão: 3/5

Combina volume de golpes, takedowns e clinch como indicador de
quem dita o ritmo e a posição da luta. Lutadores que pressionam
mais tendem a vencer por pontos ou desgaste.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def score(d: DadosLutador) -> float:
        golpes = _v(d.stats, "media_golpes_por_round")
        tds    = _v(d.stats, "media_takedowns")
        clinch = _v(d.stats, "media_clinch")
        return round(golpes * 0.5 + tds * 3.0 + clinch * 1.5, 2)

    sa = score(a)
    sb = score(b)
    return {
        "score_pressao_a": sa,
        "score_pressao_b": sb,
        "maior_pressao":   a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
