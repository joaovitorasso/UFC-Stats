"""
Módulo: Durabilidade / Queixo
Peso padrão: 3/5

Avalia a capacidade do lutador de absorver punição e ir a distância:
- Taxa de derrotas por KO/TKO (queixo frágil = maior risco)
- Percentual de lutas que foram a distância completa
- Capacidade de virar lutas quando está em desvantagem

Lutadores duráveis raramente caem por KO e frequentemente chegam
ao final da luta, o que lhes dá mais "margem de erro" na análise.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 3


def _analise_lutador(d: DadosLutador) -> dict:
    hist = d.historico

    if hist.empty:
        return {
            "lutas_total":           0,
            "lutas_a_distancia":     0,
            "pct_distancia":         0.0,
            "derrotas_por_ko":       0,
            "pct_derrotas_ko":       0.0,
            "score_durabilidade":    50.0,
            "perfil_durabilidade":   "sem dados",
        }

    total = len(hist)
    perdas = hist[hist["resultado"].isin(["loss", "L"])]
    perdas_ko = perdas[perdas["method_short"].str.contains("KO|TKO", na=False)] if not perdas.empty else perdas

    # Lutas a distância: method_short contém "Decision"
    dist = hist[hist["method_short"].str.contains("Decision", na=False)]
    pct_dist = round(len(dist) / total * 100, 1)

    total_perdas = max(len(perdas), 1)
    pct_ko = round(len(perdas_ko) / total_perdas * 100, 1)

    # Score: base 60 + bônus por ir a distância - penalidade por queixo frágil
    score = 60.0 + (pct_dist - 40) * 0.3 - pct_ko * 0.4
    score = max(0.0, min(100.0, round(score, 1)))

    if pct_ko >= 40:
        perfil = "queixo frágil"
    elif pct_dist >= 60:
        perfil = "muito durável"
    elif pct_dist >= 40:
        perfil = "durável"
    else:
        perfil = "mediano"

    return {
        "lutas_total":         total,
        "lutas_a_distancia":   len(dist),
        "pct_distancia":       pct_dist,
        "derrotas_por_ko":     len(perdas_ko),
        "pct_derrotas_ko":     pct_ko,
        "score_durabilidade":  score,
        "perfil_durabilidade": perfil,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    sa = info_a["score_durabilidade"]
    sb = info_b["score_durabilidade"]

    return {
        a.nome:        info_a,
        b.nome:        info_b,
        "mais_duravel": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
