"""
Módulo: Tendência — Últimas 5 Lutas
Peso padrão: 3/5

Calcula um score de momentum ponderando as lutas mais recentes
com peso maior (5→1). Captura forma atual e recuperação pós-derrota,
fatores que não aparecem nos agregados históricos.
"""
from __future__ import annotations

import pandas as pd
from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def streak(hist: pd.DataFrame) -> dict:
        if hist.empty:
            return {"sequencia": "N/A", "vitorias_recentes": 0, "derrotas_recentes": 0, "momentum": 0}
        ultimas  = hist.head(5)
        wins     = int(ultimas["resultado"].isin(["win", "W"]).sum())
        losses   = int(ultimas["resultado"].isin(["loss", "L"]).sum())
        momentum = 0
        for i, (_, row) in enumerate(ultimas.iterrows()):
            peso = 5 - i
            if row["resultado"] in ["win", "W"]:
                momentum += peso
            elif row["resultado"] in ["loss", "L"]:
                momentum -= peso
        seq = " ".join(
            "V" if r in ["win", "W"] else ("D" if r in ["loss", "L"] else "?")
            for r in ultimas["resultado"].tolist()
        )
        return {"sequencia": seq, "vitorias_recentes": wins, "derrotas_recentes": losses, "momentum": momentum}

    ta = streak(a.historico)
    tb = streak(b.historico)
    return {
        a.nome:         ta,
        b.nome:         tb,
        "melhor_momento": (
            a.nome if ta["momentum"] > tb["momentum"] else (
                b.nome if tb["momentum"] > ta["momentum"] else "Empate"
            )
        ),
    }
