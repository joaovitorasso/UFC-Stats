"""
Módulo: Controle de Solo / Grappling
Peso padrão: 3/5

Mede domínio no ground game através de takedowns, tentativas de
submissão e golpes no solo. Peso moderado pois é determinante
para grapplers mas irrelevante em lutas estritamente em pé.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def score(d: DadosLutador) -> float:
        tds    = _v(d.stats, "media_takedowns")
        subs   = _v(d.stats, "media_tentativas_sub")
        ground = _v(d.stats, "media_ground")
        return round(tds * 2.5 + subs * 1.5 + ground * 0.5, 2)

    sa = score(a)
    sb = score(b)
    return {
        "score_grappling_a": sa,
        "score_grappling_b": sb,
        "dominante_solo":    a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
        "takedowns_a":       round(_v(a.stats, "media_takedowns"), 2),
        "takedowns_b":       round(_v(b.stats, "media_takedowns"), 2),
        "subs_a":            round(_v(a.stats, "media_tentativas_sub"), 2),
        "subs_b":            round(_v(b.stats, "media_tentativas_sub"), 2),
    }
