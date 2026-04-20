"""
Módulo: Timing de Finalização
Peso padrão: 2/5

Analisa em qual round e por qual método cada lutador costuma finalizar.
Peso baixo pois finalizações são eventos raros; volume de dados
estatisticamente pequeno para a maioria dos lutadores.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 2


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def resumo(d: DadosLutador) -> dict:
        if d.timing_finalizacao.empty:
            return {"finalizacoes": 0, "metodo_principal": "N/A", "round_principal": "N/A"}
        top   = d.timing_finalizacao.iloc[0]
        total = int(d.timing_finalizacao["quantidade"].sum())
        return {
            "finalizacoes":      total,
            "metodo_principal":  str(top["metodo"]),
            "round_principal":   int(top["round_fim"]) if top["round_fim"] is not None else "N/A",
        }

    r_a = resumo(a)
    r_b = resumo(b)
    return {
        a.nome:       r_a,
        b.nome:       r_b,
        "finalizador": (
            a.nome if r_a["finalizacoes"] > r_b["finalizacoes"] else (
                b.nome if r_b["finalizacoes"] > r_a["finalizacoes"] else "Empate"
            )
        ),
    }
