"""
Módulo: Médias de Apostas (Proxy Histórico)
Peso padrão: 3/5

Proxy estatístico de favorito baseado em win rate ponderado e
experiência acumulada. Sem dados reais de odds; substitui esse
sinal com histórico quantitativo que tem correlação similar.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def score(d: DadosLutador) -> float:
        wr          = _v(d.stats, "win_rate_pct")
        wins        = _v(d.stats, "vitorias")
        losses      = _v(d.stats, "derrotas")
        experiencia = min((wins + losses) / 30, 1.0) * 10
        return round(wr * 0.8 + experiencia * 0.2, 2)

    sa    = score(a)
    sb    = score(b)
    total = max(sa + sb, 1.0)
    return {
        "score_historico_a":         sa,
        "score_historico_b":         sb,
        "probabilidade_implicita_a": round(sa / total * 100, 1),
        "probabilidade_implicita_b": round(sb / total * 100, 1),
        "nota": "Baseado em win rate e experiência. Sem dados reais de odds.",
    }
