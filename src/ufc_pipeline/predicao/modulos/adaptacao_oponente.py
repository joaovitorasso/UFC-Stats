"""
Módulo: Adaptação a Oponentes
Peso padrão: 2/5

Analisa o histórico geral (win rate, métodos de vitória) como proxy
de adaptabilidade tática. Peso baixo pois não distingue qualidade
dos oponentes anteriores.
"""
from __future__ import annotations

import pandas as pd
from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 2


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def por_estilo(hist: pd.DataFrame) -> dict:
        if hist.empty:
            return {}
        wins   = hist[hist["resultado"].isin(["win", "W"])]
        losses = hist[hist["resultado"].isin(["loss", "L"])]
        return {
            "total":    len(hist),
            "vitorias": len(wins),
            "derrotas": len(losses),
            "win_rate": round(len(wins) / max(len(hist), 1) * 100, 1),
            "ko_dado":  int(wins["method_short"].str.contains("KO|TKO", na=False).sum()),
            "sub_dado": int(wins["method_short"].str.contains("Sub", na=False).sum()),
        }

    return {
        a.nome: por_estilo(a.historico),
        b.nome: por_estilo(b.historico),
    }
