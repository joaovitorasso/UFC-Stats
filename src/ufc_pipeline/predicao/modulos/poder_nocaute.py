"""
Módulo: Poder de Nocaute
Peso padrão: 3/5

Mede a ameaça de finalização por KO/TKO combinando:
- Taxa de vitórias por KO/TKO na carreira
- Média de knockdowns dados por luta
- Taxa de finalizações em geral (KO + Sub)

Útil para identificar lutadores com alto poder de nocaute que
podem encerrar a luta a qualquer momento, mesmo em desvantagem
nos pontos.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def _analise_lutador(d: DadosLutador) -> dict:
    hist = d.historico
    wins = hist[hist["resultado"].isin(["win", "W"])] if not hist.empty else hist

    total_lutas   = max(len(hist), 1)
    total_vitorias = max(len(wins), 1)

    ko_wins = wins[wins["method_short"].str.contains("KO|TKO", na=False)] if not wins.empty else wins
    sub_wins = wins[wins["method_short"].str.contains("Sub", na=False)] if not wins.empty else wins

    ko_rate      = round(len(ko_wins) / total_vitorias * 100, 1)
    finish_rate  = round((len(ko_wins) + len(sub_wins)) / total_vitorias * 100, 1)
    ko_por_carreira = len(ko_wins)

    media_kd = _v(d.stats, "media_knockdowns")

    # Score composto: KO rate + knockdowns dados + finish rate
    score = round(ko_rate * 0.4 + media_kd * 30 + finish_rate * 0.3, 2)

    # Vulnerabilidade: quantas vezes foi KO'd
    perdas_ko = hist[
        hist["resultado"].isin(["loss", "L"]) &
        hist["method_short"].str.contains("KO|TKO", na=False)
    ] if not hist.empty else hist

    return {
        "ko_na_carreira":  ko_por_carreira,
        "ko_rate_pct":     ko_rate,
        "finish_rate_pct": finish_rate,
        "media_knockdowns": round(media_kd, 2),
        "vezes_ko_sofrido": len(perdas_ko),
        "score_poder":     score,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    sa = info_a["score_poder"]
    sb = info_b["score_poder"]

    return {
        a.nome:    info_a,
        b.nome:    info_b,
        "vantagem": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
