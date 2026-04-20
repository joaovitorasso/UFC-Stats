"""
Módulo: Round a Round — Volume de Golpes
Peso padrão: 4/5

Compara o volume médio de golpes significativos por round.
Alto peso pois volume ofensivo consistente é o preditor mais direto
de controle da luta e vitórias por pontos ou finalização.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 4


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    rounds = sorted(set(
        list(a.curva_fadiga["round_number"].tolist()) +
        list(b.curva_fadiga["round_number"].tolist())
    ))
    resultado = {}
    vantagem_a = 0
    vantagem_b = 0
    for rnd in rounds:
        row_a = a.curva_fadiga[a.curva_fadiga["round_number"] == rnd]
        row_b = b.curva_fadiga[b.curva_fadiga["round_number"] == rnd]
        golpes_a = float(row_a["media_golpes"].iloc[0]) if not row_a.empty else 0.0
        golpes_b = float(row_b["media_golpes"].iloc[0]) if not row_b.empty else 0.0
        vencedor = a.nome if golpes_a > golpes_b else (b.nome if golpes_b > golpes_a else "Empate")
        resultado[f"R{rnd}"] = {
            "golpes_a": round(golpes_a, 1),
            "golpes_b": round(golpes_b, 1),
            "vantagem": vencedor,
        }
        if golpes_a > golpes_b:
            vantagem_a += 1
        elif golpes_b > golpes_a:
            vantagem_b += 1
    return {
        "por_round":        resultado,
        "rounds_vantagem_a": vantagem_a,
        "rounds_vantagem_b": vantagem_b,
    }
