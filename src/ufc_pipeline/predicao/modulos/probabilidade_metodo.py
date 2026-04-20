"""
Módulo: Probabilidade de Método de Vitória
Peso padrão: 2/5

Cruza o perfil ofensivo de A (KO/Sub/Decisão) com as vulnerabilidades
de B para estimar como cada lutador pode vencer. Peso baixo pois
a previsão de método tem alta variância mesmo com bons dados históricos.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 2


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def pcts(d: DadosLutador) -> dict:
        pm = d.prob_metodo
        return {
            "ko_ataque":  _v(pm, "pct_v_ko_tko"),
            "sub_ataque": _v(pm, "pct_v_sub"),
            "dec_ataque": _v(pm, "pct_v_decisao"),
            "ko_vuln":    _v(pm, "pct_d_ko_tko"),
            "sub_vuln":   _v(pm, "pct_d_sub"),
            "dec_vuln":   _v(pm, "pct_d_decisao"),
        }

    pa = pcts(a)
    pb = pcts(b)
    return {
        f"vitoria_{a.nome}_por_KO":       round(pa["ko_ataque"]  * pb["ko_vuln"]  / 100, 1),
        f"vitoria_{a.nome}_por_Sub":      round(pa["sub_ataque"] * pb["sub_vuln"] / 100, 1),
        f"vitoria_{a.nome}_por_Decisao":  pa["dec_ataque"],
        f"vitoria_{b.nome}_por_KO":       round(pb["ko_ataque"]  * pa["ko_vuln"]  / 100, 1),
        f"vitoria_{b.nome}_por_Sub":      round(pb["sub_ataque"] * pa["sub_vuln"] / 100, 1),
        f"vitoria_{b.nome}_por_Decisao":  pb["dec_ataque"],
    }
